package app.vitune.android.service

import android.util.Log
import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readText
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import org.json.JSONArray
import org.json.JSONObject
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Shows the currently playing song as a "Listening to ViTune" activity on the user's Discord
 * profile, by holding a Discord Gateway connection authenticated with the user's own token.
 *
 * Note: this uses a user account token, which Discord's Terms of Service do not allow
 * ("self-botting"). The user opts in explicitly in the settings.
 */
class DiscordPresence(
    private val scope: CoroutineScope,
    /** Looks up a song's album when the player didn't provide one. */
    private val albumLookup: suspend (songId: String) -> String? = { null }
) {
    /** What we want Discord to show. `null` means "show nothing". */
    data class Track(
        val id: String,
        val title: String,
        val artist: String?,
        val album: String?,
        val artworkUrl: String?,
        val positionMs: Long,
        val durationMs: Long,
        val isLocal: Boolean
    )

    /** Connection state, shown in the settings so problems are visible. */
    enum class Status { Off, Connecting, Connected, Reconnecting, TokenRejected }

    private val client = HttpClient(OkHttp) {
        install(WebSockets)
        // Lets OkHttp notice a dead socket on its own, in addition to the gateway heartbeat check.
        engine { config { pingInterval(30, java.util.concurrent.TimeUnit.SECONDS) } }
    }

    /** Signalled when the phone gets a network back, to skip the remaining retry wait. */
    private val networkAvailable = Channel<Unit>(Channel.CONFLATED)

    /** A track plus the wall-clock time at which it (virtually) started playing. */
    private data class Shown(val track: Track, val startedAt: Long)

    private val desired = MutableStateFlow<Shown?>(null)
    private var connectionJob: Job? = null
    private var token: String? = null
    private var applicationId: String? = null

    // Discord can only show images it proxies itself, so remote artwork URLs are converted
    // into "mp:external/..." asset keys once and cached.
    private val artworkCache = object : LinkedHashMap<String, String>(32, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, String>?) = size > 64
    }
    private val artworkMutex = Mutex()

    /** Starts (or restarts) the gateway connection. Call with `token = null` to stop. */
    fun configure(token: String?, applicationId: String?) {
        val newToken = token?.trim()?.takeIf { it.isNotEmpty() }
        val newAppId = applicationId?.trim()?.takeIf { it.isNotEmpty() }
        if (newToken == this.token && newAppId == this.applicationId && connectionJob?.isActive == true) return

        this.token = newToken
        this.applicationId = newAppId

        val previous = connectionJob
        connectionJob = scope.launch {
            previous?.cancelAndJoin()
            if (newToken != null) runConnectionLoop(newToken) else mutableStatus.value = Status.Off
        }
    }

    /** Call when the device (re)gains a network connection. */
    fun onNetworkAvailable() {
        networkAvailable.trySend(Unit)
    }

    fun update(track: Track?) {
        if (track == null) {
            desired.value = null
            return
        }

        val startedAt = System.currentTimeMillis() - track.positionMs
        val current = desired.value
        // The player reports many events with slightly different positions; only push an update
        // when the song changed or the user actually seeked.
        if (
            current != null &&
            current.track.copy(positionMs = 0) == track.copy(positionMs = 0) &&
            kotlin.math.abs(current.startedAt - startedAt) < SEEK_TOLERANCE_MS
        ) return

        desired.value = Shown(track, startedAt)
    }

    fun release() {
        connectionJob?.cancel()
        connectionJob = null
        mutableStatus.value = Status.Off
        client.close()
    }

    private suspend fun runConnectionLoop(token: String) {
        var backoff = 2.seconds
        mutableStatus.value = Status.Connecting
        while (scope.isActive) {
            val connectedAt = System.currentTimeMillis()
            val closeCode = runCatching { connectOnce(token) }
                .onFailure {
                    if (it is CancellationException) throw it
                    Log.w(TAG, "Gateway connection failed", it)
                }
                .getOrNull()

            if (closeCode == CLOSE_AUTHENTICATION_FAILED) {
                Log.e(TAG, "Discord rejected the token; Rich Presence stopped")
                mutableStatus.value = Status.TokenRejected
                return
            }
            if (closeCode != null && closeCode in FATAL_CLOSE_CODES) {
                Log.e(TAG, "Gateway closed with fatal code $closeCode; Rich Presence stopped")
                mutableStatus.value = Status.Off
                return
            }

            mutableStatus.value = Status.Reconnecting
            Log.i(TAG, "Gateway disconnected (code $closeCode), reconnecting in $backoff")

            // A connection that stayed up for a while was healthy, so start over with a short wait.
            if (System.currentTimeMillis() - connectedAt > 60_000) backoff = 2.seconds

            // Drop a stale "network is back" signal, then wait, waking early if the network returns.
            networkAvailable.tryReceive()
            withTimeoutOrNull(backoff) { networkAvailable.receive() }
            backoff = (backoff * 2).coerceAtMost(60.seconds)
        }
    }

    /** Returns the gateway close code, if the server closed the connection. */
    @OptIn(FlowPreview::class)
    @Suppress("CyclomaticComplexMethod", "LongMethod")
    private suspend fun connectOnce(token: String): Short? {
        var closeCode: Short? = null

        client.webSocket(GATEWAY_URL) {
            var sequence: Int? = null
            var closeLocally = false
            // Whether Discord acknowledged our last heartbeat. If it didn't by the time the next
            // one is due, the connection is dead (common after a network switch or Doze), even
            // though the socket still looks open, so we drop it and reconnect.
            var heartbeatAcked = true

            coroutineScope {
                var heartbeatJob: Job? = null
                var presenceJob: Job? = null

                try {
                    for (frame in incoming) {
                        if (frame !is Frame.Text) continue
                        val payload = JSONObject(frame.readText())
                        if (!payload.isNull("s")) sequence = payload.getInt("s")

                        when (payload.getInt("op")) {
                            OP_HELLO -> {
                                val interval = payload.getJSONObject("d")
                                    .getLong("heartbeat_interval").milliseconds

                                heartbeatJob?.cancel()
                                heartbeatJob = launch {
                                    delay(interval * Math.random())
                                    while (isActive) {
                                        // Failing here tears down this connection, and the
                                        // loop in runConnectionLoop opens a fresh one.
                                        if (!heartbeatAcked) throw ZombieConnectionException()
                                        heartbeatAcked = false
                                        sendJson(heartbeat(sequence))
                                        delay(interval)
                                    }
                                }

                                sendJson(identify(token))

                                presenceJob?.cancel()
                                presenceJob = launch {
                                    desired
                                        .debounce(1.seconds)
                                        .distinctUntilChanged()
                                        .collect { sendJson(presenceUpdate(it, token)) }
                                }
                            }

                            OP_DISPATCH -> if (payload.optString("t") == "READY") {
                                mutableStatus.value = Status.Connected
                            }
                            OP_HEARTBEAT -> sendJson(heartbeat(sequence))
                            OP_HEARTBEAT_ACK -> heartbeatAcked = true
                            OP_RECONNECT, OP_INVALID_SESSION -> {
                                closeLocally = true
                                break
                            }
                        }
                    }
                } finally {
                    heartbeatJob?.cancel()
                    presenceJob?.cancel()
                }
            }

            if (closeLocally) close(CloseReason(CloseReason.Codes.NORMAL, "reconnecting"))
            else closeCode = closeReason.await()?.code
        }

        return closeCode
    }

    private suspend fun DefaultClientWebSocketSession.sendJson(json: JSONObject) =
        send(Frame.Text(json.toString()))

    private fun heartbeat(sequence: Int?) = JSONObject()
        .put("op", OP_HEARTBEAT)
        .put("d", sequence ?: JSONObject.NULL)

    private fun identify(token: String) = JSONObject()
        .put("op", OP_IDENTIFY)
        .put(
            "d",
            JSONObject()
                .put("token", token)
                .put("capabilities", 65)
                .put("compress", false)
                .put(
                    "properties",
                    JSONObject()
                        .put("os", "Android")
                        .put("browser", "Discord Android")
                        .put("device", android.os.Build.MODEL ?: "Android")
                )
        )

    private suspend fun presenceUpdate(shown: Shown?, token: String): JSONObject {
        val activities = JSONArray()
        if (shown != null) activities.put(activity(shown.track, shown.startedAt, token))

        return JSONObject()
            .put("op", OP_PRESENCE_UPDATE)
            .put(
                "d",
                JSONObject()
                    .put("since", 0)
                    .put("activities", activities)
                    .put("status", "online")
                    .put("afk", false)
            )
    }

    private suspend fun activity(track: Track, start: Long, token: String): JSONObject {
        val activity = JSONObject()
            .put("name", APP_NAME)
            .put("type", ACTIVITY_TYPE_LISTENING)
            .put("details", track.title.forDiscord())
            .put("state", (track.artist ?: APP_NAME).forDiscord())
            .put(
                "timestamps",
                JSONObject().apply {
                    put("start", start)
                    if (track.durationMs > 0) put("end", start + track.durationMs)
                }
            )

        // The album shows as the third line under the artist; leave it out rather than repeat the title.
        val album = track.album?.takeIf { it.isNotBlank() }
            ?: runCatching { albumLookup(track.id) }.getOrNull()?.takeIf { it.isNotBlank() }

        applicationId?.let { appId ->
            activity.put("application_id", appId)

            val assetKey = track.artworkUrl?.let { resolveArtwork(it, appId, token) }
            if (assetKey != null) activity.put(
                "assets",
                JSONObject().apply {
                    put("large_image", assetKey)
                    if (album != null && album != track.title) put("large_text", album.forDiscord())
                }
            )
        }

        if (!track.isLocal) activity
            .put("buttons", JSONArray().put("Listen on YouTube Music"))
            .put(
                "metadata",
                JSONObject().put(
                    "button_urls",
                    JSONArray().put("https://music.youtube.com/watch?v=${track.id}")
                )
            )

        return activity
    }

    private suspend fun resolveArtwork(url: String, appId: String, token: String): String? {
        if (!url.startsWith("http")) return null // local content:// URIs can't be shown
        artworkMutex.withLock { artworkCache[url] }?.let { return it }

        return runCatching {
            val response = client.post("https://discord.com/api/v9/applications/$appId/external-assets") {
                header("Authorization", token)
                contentType(ContentType.Application.Json)
                setBody(JSONObject().put("urls", JSONArray().put(url)).toString())
            }
            if (!response.status.isSuccess()) return@runCatching null

            val path = JSONArray(response.bodyAsText())
                .optJSONObject(0)
                ?.optString("external_asset_path")
                ?.takeIf { it.isNotEmpty() }
                ?: return@runCatching null

            "mp:$path".also { key -> artworkMutex.withLock { artworkCache[url] = key } }
        }.onFailure {
            if (it is CancellationException) throw it
            Log.w(TAG, "Could not upload artwork to Discord", it)
        }.getOrNull()
    }

    /** Discord requires 2..128 characters for activity text fields. */
    private fun String.forDiscord() = take(128).padEnd(2, ' ')

    private class ZombieConnectionException :
        IllegalStateException("Discord stopped acknowledging heartbeats")

    companion object {
        private val mutableStatus = MutableStateFlow(Status.Off)
        val status: StateFlow<Status> = mutableStatus.asStateFlow()

        private const val TAG = "DiscordPresence"
        private const val APP_NAME = "ViTune"
        private const val GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"

        private const val OP_DISPATCH = 0
        private const val OP_HEARTBEAT = 1
        private const val OP_IDENTIFY = 2
        private const val OP_PRESENCE_UPDATE = 3
        private const val OP_RECONNECT = 7
        private const val OP_INVALID_SESSION = 9
        private const val OP_HELLO = 10
        private const val OP_HEARTBEAT_ACK = 11

        private const val ACTIVITY_TYPE_LISTENING = 2
        private const val SEEK_TOLERANCE_MS = 3_000L
        private const val CLOSE_AUTHENTICATION_FAILED: Short = 4004

        /** Close codes after which reconnecting cannot help (bad intents, gateway version, etc.). */
        private val FATAL_CLOSE_CODES = setOf<Short>(4010, 4011, 4012, 4013, 4014)
    }
}
