package app.vitune.android.discord

import app.vitune.android.GlobalPreferencesHolder

object DiscordPreferences : GlobalPreferencesHolder() {
    var enabled by boolean(false)
}
