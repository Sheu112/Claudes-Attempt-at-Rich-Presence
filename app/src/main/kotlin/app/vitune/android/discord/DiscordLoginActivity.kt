package app.vitune.android.discord

import android.app.Activity
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.webkit.CookieManager
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Toast

class DiscordLoginActivity : Activity() {

    private lateinit var webView: WebView
    private val handler = Handler(Looper.getMainLooper())
    private var done = false

    private val js = """
        (function() {
            try {
                var i = document.createElement('iframe');
                document.body.appendChild(i);
                var t = i.contentWindow.localStorage.getItem('token');
                if (t) return t;
            } catch (e) {}
            return '';
        })()
    """.trimIndent()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        webView = WebView(this).apply {
            settings.javaScriptEnabled = true
            settings.domStorageEnabled = true
        }
        setContentView(webView)

        CookieManager.getInstance().removeAllCookies(null)
        webView.webViewClient = object : WebViewClient() {
            override fun doUpdateVisitedHistory(view: WebView, url: String, isReload: Boolean) {
                if (url.contains("/channels/") || url.endsWith("/app")) tryGrab(0)
            }
        }
        webView.loadUrl("https://discord.com/login")
    }

    private fun tryGrab(attempt: Int) {
        if (done || attempt > 15) return
        webView.evaluateJavascript(js) { raw ->
            val token = raw?.replace("\"", "")?.replace("\\", "")?.trim().orEmpty()
            if (token.length > 20 && token != "null") {
                done = true
                DiscordPrefs.setToken(this, token)
                Toast.makeText(this, "Discord connected", Toast.LENGTH_SHORT).show()
                finish()
            } else {
                handler.postDelayed({ tryGrab(attempt + 1) }, 1000)
            }
        }
    }

    override fun onDestroy() {
        handler.removeCallbacksAndMessages(null)
        webView.destroy()
        super.onDestroy()
    }
}
