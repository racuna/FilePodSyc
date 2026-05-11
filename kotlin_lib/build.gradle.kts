// build.gradle.kts (app module)
plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    kotlin("plugin.serialization") version "1.9.22"
}

dependencies {
    // Core
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.2")

    // Android architecture
    implementation("androidx.lifecycle:lifecycle-runtime-ktx:2.7.0")
    implementation("androidx.lifecycle:lifecycle-viewmodel-ktx:2.7.0")
    implementation("androidx.work:work-runtime-ktx:2.9.0")

    // Optional: if you want type-safe shared preferences for device ID
    implementation("androidx.datastore:datastore-preferences:1.0.0")

    // For OPML parsing (import only)
    implementation("org.xmlpull:xmlpull:1.1.3.1")
}
