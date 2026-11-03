package app.vitune.android.discord

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONArray
import org.json.JSONObject
import kotlin.random.Random

class DiscordGateway(
    private val token: String,
    private val scope: CoroutineScope
) {
    private val client = OkHttpClient()
    private var socket: WebSocket? = null
    private var heartbeat: Job? = null
    private var reconnect: Job? = null
    private var seq: Int? = null
    private var backoff = 2_000L

    @Volatile private var ready = false
    @Volatile private var closed = false
    @Volatile private var activity: JSONObject? = null

    fun connect() {
        closed = false
        open()
    }

    fun setActivity(a: JSONObject?) {
        activity = a
        if (ready) sendPresence()
    }

    fun close() {
        closed = true
        heartbeat?.cancel()
        reconnect?.cancel()
        activity = null
        if (ready) sendPresence()
        socket?.close(1000, null)
        socket = null
        ready = false
    }

    private fun open() {
        ready = false
        socket = client.newWebSocket(
            Request.Builder().url("wss://gateway.discord.gg/?v=10&encoding=json").build(),
            listener
        )
    }

    private fun presencePayload() = JSONObject()
        .put("since", JSONObject.NULL)
        .put("activities", JSONArray().apply { activity?.let { put(it) } })
        .put("status", "online")
        .put("afk", false)

    private fun sendPresence() {
        socket?.send(JSONObject().put("op", 3).put("d", presencePayload()).toString())
    }

    private fun identify(ws: WebSocket) {
        val d = JSONObject()
            .put("token", token)
            .put("capabilities", 65)
            .put("compress", false)
            .put(
                "properties",
                JSONObject()
                    .put("os", "Android")
                    .put("browser", "Discord Android")
                    .put("device", "Discord Android")
            )
            .put("presence", presencePayload())
        ws.send(JSONObject().put("op", 2).put("d", d).toString())
    }

    private fun sendHeartbeat(ws: WebSocket) {
        ws.send(JSONObject().put("op", 1).put("d", seq ?: JSONObject.NULL).toString())
    }

    private fun startHeartbeat(interval: Long) {
        heartbeat?.cancel()
        heartbeat = scope.launch {
            delay((interval * Random.nextDouble()).toLong())
            while (isActive) {
                socket?.let { sendHeartbeat(it) }
                delay(interval)
            }
        }
    }

    private fun scheduleReconnect() {
        if (closed || reconnect?.isActive == true) return
        heartbeat?.cancel()
        ready = false
        reconnect = scope.launch {
            delay(backoff)
            backoff = (backoff * 2).coerceAtMost(60_000L)
            if (!closed) open()
        }
    }

    private val listener = object : WebSocketListener() {
        override fun onMessage(webSocket: WebSocket, text: String) {
            val j = JSONObject(text)
            j.optInt("s", -1).takeIf { it >= 0 }?.let { seq = it }
            when (j.getInt("op")) {
                10 -> {
                    startHeartbeat(j.getJSONObject("d").getLong("heartbeat_interval"))
                    identify(webSocket)
                }
                1 -> sendHeartbeat(webSocket)
                0 -> if (j.optString("t") == "READY") {
                    ready = true
                    backoff = 2_000L
                    sendPresence()
                }
                7, 9 -> webSocket.close(4000, "reconnect")
            }
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) = scheduleReconnect()
        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) = scheduleReconnect()
    }
}
