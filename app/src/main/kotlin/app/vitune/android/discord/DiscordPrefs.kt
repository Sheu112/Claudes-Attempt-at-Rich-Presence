package app.vitune.android.discord

import android.content.Context

object DiscordPrefs {
    private fun sp(c: Context) = c.getSharedPreferences("discord_presence", Context.MODE_PRIVATE)

    fun token(c: Context): String? = sp(c).getString("token", null)?.takeIf { it.isNotBlank() }
    fun setToken(c: Context, t: String?) = sp(c).edit().putString("token", t).apply()
    fun logout(c: Context) = setToken(c, null)
}
