package com.filepodsync.android

import android.content.Context
import androidx.work.*
import com.filepodsync.core.FilePodSyncManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.util.concurrent.TimeUnit

/**
 * WorkManager-based background sync for FilePodSync on Android.
 *
 * Schedules periodic syncs every 15 minutes (minimum interval for
 * PeriodicWorkRequest), plus immediate one-shot syncs on app events.
 *
 * Usage in Application.onCreate():
 * ```
 * FilePodSyncWorker.schedulePeriodic(context, File(context.filesDir, "fps"))
 * ```
 */
class FilePodSyncWorker(
    context: Context,
    params: WorkerParameters
) : CoroutineWorker(context, params) {

    companion object {
        private const val WORK_NAME_PERIODIC = "filepodsync_periodic"
        private const val KEY_SYNC_DIR = "sync_dir"

        fun schedulePeriodic(context: Context, syncDir: File) {
            val constraints = Constraints.Builder()
                .setRequiredNetworkType(NetworkType.CONNECTED)
                .build()

            val request = PeriodicWorkRequestBuilder<FilePodSyncWorker>(
                15, TimeUnit.MINUTES,
                5, TimeUnit.MINUTES  // flex interval
            )
                .setInputData(workDataOf(KEY_SYNC_DIR to syncDir.absolutePath))
                .setConstraints(constraints)
                .build()

            WorkManager.getInstance(context).enqueueUniquePeriodicWork(
                WORK_NAME_PERIODIC,
                ExistingPeriodicWorkPolicy.KEEP,
                request
            )
        }

        fun syncNow(context: Context, syncDir: File) {
            val request = OneTimeWorkRequestBuilder<FilePodSyncWorker>()
                .setInputData(workDataOf(KEY_SYNC_DIR to syncDir.absolutePath))
                .build()
            WorkManager.getInstance(context).enqueue(request)
        }
    }

    override suspend fun doWork(): Result = withContext(Dispatchers.IO) {
        val syncDirPath = inputData.getString(KEY_SYNC_DIR) ?: return@withContext Result.failure()
        val syncDir = File(syncDirPath)

        val fps = FilePodSyncManager(
            context = applicationContext,
            syncDir = syncDir,
            deviceName = android.os.Build.MODEL ?: "Android Device"
        )

        try {
            fps.sync()
            fps.housekeeping()
            Result.success()
        } catch (e: Exception) {
            android.util.Log.e("FilePodSyncWorker", "Sync failed", e)
            Result.retry()
        } finally {
            fps.shutdown()
        }
    }
}
