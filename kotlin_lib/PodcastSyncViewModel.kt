package com.filepodsync.android

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.filepodsync.core.FilePodSyncManager
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import java.io.File

/**
 * Example ViewModel showing how to integrate FilePodSyncManager with Android UI.
 *
 * Exposes reactive StateFlows for feeds, episodes, and queue.
 * All operations are main-safe (offloaded to Dispatchers.IO internally).
 */
class PodcastSyncViewModel(
    private val fps: FilePodSyncManager
) : ViewModel() {

    val feeds: StateFlow<Map<String, FilePodSyncManager.FeedInfo>> =
        fps.feeds.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyMap())

    val episodes: StateFlow<Map<String, FilePodSyncManager.EpisodeInfo>> =
        fps.episodes.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyMap())

    val queue: StateFlow<List<FilePodSyncManager.QueueItem>> =
        fps.queue.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    fun addFeed(url: String, title: String = "") {
        viewModelScope.launch {
            fps.addFeed(url, title)
        }
    }

    fun updateProgress(episodeUrl: String, feedUrl: String, guid: String?, position: Int, total: Int) {
        viewModelScope.launch {
            fps.updateEpisode(
                episodeUrl = episodeUrl,
                feedUrl = feedUrl,
                guid = guid,
                position = position,
                total = total,
                state = if (position >= total - 10) "completed" else "in_progress"
            )
        }
    }

    fun addToQueue(epId: String) {
        fps.queueAdd(epId)
    }

    fun sync() {
        viewModelScope.launch {
            fps.sync()
        }
    }

    override fun onCleared() {
        super.onCleared()
        viewModelScope.launch {
            fps.shutdown()
        }
    }
}
