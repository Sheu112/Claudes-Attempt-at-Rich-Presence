package app.vitune.android.discord

import android.content.Context
import androidx.media3.common.C
import androidx.media3.common.MediaItem
import androidx.media3.common.Player
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject

class DiscordPresenceManager(
    private val context: Context,
    private val player: Player
) : Player.Listener {

    companion object {
        private const val APP_ID = "1567200400707166208"
        private const val APP_NAME = "ViTune"
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val http = OkHttpClient()
    private val imageCache = HashMap<String, String>()

    private var gateway: DiscordGateway? = null
    private var gatewayToken: String? = null

    override fun onMediaItemTransition(mediaItem: MediaItem?, reason: Int) = update()
    override fun onIsPlayingChanged(isPlaying: Boolean) = update()
    override fun onPositionDiscontinuity(
        oldPosition: Player.PositionInfo,
        newPosition: Player.PositionInfo,
        reason: Int
    ) = update()

    private fun update() {
        val token = DiscordPrefs.token(context)
        if (!DiscordPreferences.enabled || token == null) {
            shutdownGateway()
            return
        }

        val gw = ensureGateway(token)

        val item = player.currentMediaItem
        if (!player.isPlaying || item == null) {
            gw.setActivity(null)
            return
        }

        val md = item.mediaMetadata
        val title = md.title?.toString() ?: "Unknown"
        val artist = md.artist?.toString() ?: "Unknown artist"
        val album = md.albumTitle?.toString()
        val art = md.artworkUri?.toString()
        val duration = player.duration
        val start = System.currentTimeMillis() - player.currentPosition
        val end = if (duration != C.TIME_UNSET && duration > 0) start + duration else null

        scope.launch {
            val image = art?.let { resolveImage(token, it) }
            val activity = JSONObject()
                .put("name", APP_NAME)
                .put("type", 2)
                .put("application_id", APP_ID)
                .put("details", title)
                .put("state", artist)
                .put(
                    "timestamps",
                    JSONObject().put("start", start).apply { end?.let { put("end", it) } }
                )
            if (image != null) {
                activity.put(
                    "assets",
                    JSONObject().put("large_image", image).put("large_text", album ?: title)
                )
            }
            gw.setActivity(activity)
        }
    }

    private fun ensureGateway(token: String): DiscordGateway {
        if (gateway == null || gatewayToken != token) {
            gateway?.close()
            gateway = DiscordGateway(token, scope).also { it.connect() }
            gatewayToken = token
        }
        return gateway!!
    }

    private fun shutdownGateway() {
        gateway?.close()
        gateway = null
        gatewayToken = null
    }

    private suspend fun resolveImage(token: String, url: String): String? = withContext(Dispatchers.IO) {
        imageCache[url]?.let { return@withContext it }
        runCatching {
            val body = JSONObject().put("urls", JSONArray().put(url)).toString()
                .toRequestBody("application/json".toMediaType())
            val req = Request.Builder()
                .url("https://discord.com/api/v9/applications/$APP_ID/external-assets")
                .header("Authorization", token)
                .post(body)
                .build()
            http.newCall(req).execute().use { r ->
                val path = JSONArray(r.body!!.string()).getJSONObject(0).getString("external_asset_path")
                "mp:$path"
            }
        }.getOrNull()?.also { imageCache[url] = it }
    }

    fun release() {
        shutdownGateway()
        scope.cancel()
    }
}
