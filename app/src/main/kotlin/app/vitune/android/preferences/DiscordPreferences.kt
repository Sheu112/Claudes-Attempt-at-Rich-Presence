package app.vitune.android.preferences

import app.vitune.android.Dependencies
import app.vitune.compose.preferences.PreferencesHolder

/**
 * Kept in its own SharedPreferences file ("discord") instead of the global "preferences" file,
 * so the account token is not swept up with the rest of the settings.
 */
object DiscordPreferences : PreferencesHolder(Dependencies.application, "discord") {
    val enabledProperty = boolean(false)
    var enabled by enabledProperty

    /** The user's Discord account token. Treat it like a password. */
    val tokenProperty = string("")
    var token by tokenProperty

    /**
     * ID of an application created at https://discord.com/developers/applications.
     * Needed for album art; without it the presence still shows, just without images.
     */
    val applicationIdProperty = string("")
    var applicationId by applicationIdProperty
}
