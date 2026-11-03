import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { Innertube, UniversalCache } from 'youtubei.js';
import { Ytdl } from '@distube/ytdl-core';
import { Readable } from 'node:stream';
dotenv.config();
const app = express();
const PORT = process.env.PORT || 5000;
app.use(cors({
    origin: true,
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization']
}));
app.use(express.json());
const streamCache = new Map();
let likedSongs = [];
let recentlyPlayed = [];
let playlists = [];
// Pure JavaScript YouTube Client Singleton
let yt = null;
async function getYouTube() {
    if (!yt) {
        yt = await Innertube.create({
            cache: new UniversalCache(false),
            generate_session_locally: true,
        });
        console.log('⚡ Fast Pure JS YouTube Engine ready');
    }
    return yt;
}
// Pure JS in-memory fallback using @distube/ytdl-core (zero binary dependencies)
const ytdl = new Ytdl({
    clients: ['visionos', 'web_embedded', 'tv', 'mweb'],
});
// Sanitization Helpers to guarantee primitive strings are returned
function cleanTitle(val, fallback = 'Unknown') {
    if (!val)
        return fallback;
    if (typeof val === 'string')
        return val.trim() || fallback;
    if (typeof val?.text === 'string')
        return val.text.trim() || fallback;
    if (Array.isArray(val?.runs) && val.runs.length > 0) {
        const text = val.runs.map((r) => r.text || '').join('').trim();
        if (text)
            return text;
    }
    if (typeof val?.name === 'string')
        return val.name.trim() || fallback;
    if (typeof val?.toString === 'function') {
        const str = val.toString();
        if (str && str !== '[object Object]')
            return str.trim();
    }
    return fallback;
}
function cleanArtist(val, fallback = 'Artist') {
    if (!val)
        return fallback;
    if (typeof val === 'string')
        return val.trim() || fallback;
    if (Array.isArray(val)) {
        const names = val.map((item) => {
            if (typeof item === 'string')
                return item.trim();
            return item?.name?.trim?.() || item?.text?.trim?.() || '';
        }).filter(Boolean);
        if (names.length > 0)
            return names.join(', ');
    }
    if (typeof val?.name === 'string')
        return val.name.trim() || fallback;
    if (typeof val?.text === 'string')
        return val.text.trim() || fallback;
    if (Array.isArray(val?.runs) && val.runs.length > 0) {
        const text = val.runs.map((r) => r.text || '').join('').trim();
        if (text)
            return text;
    }
    return fallback;
}
function cleanArtwork(val) {
    if (!val)
        return '';
    if (typeof val === 'string')
        return val;
    if (Array.isArray(val) && val.length > 0) {
        const last = val[val.length - 1];
        return last?.url || val[0]?.url || '';
    }
    return val.url || '';
}
let spotifyToken = '';
let tokenExpiresAt = 0;
async function getSpotifyToken() {
    if (spotifyToken && Date.now() < tokenExpiresAt)
        return spotifyToken;
    const clientId = process.env.SPOTIFY_CLIENT_ID;
    const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;
    if (!clientId || !clientSecret)
        throw new Error('Missing Spotify credentials in .env');
    const res = await fetch('https://accounts.spotify.com/api/token', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`,
        },
        body: 'grant_type=client_credentials',
    });
    if (!res.ok)
        throw new Error(`Spotify token error: ${res.status}`);
    const data = await res.json();
    spotifyToken = data.access_token;
    tokenExpiresAt = Date.now() + (data.expires_in - 300) * 1000;
    return spotifyToken;
}
// 1. Search Suggestions
app.get('/api/suggestions', async (req, res) => {
    const query = (req.query.q || '').trim();
    if (!query)
        return res.json({ suggestions: [] });
    try {
        const token = await getSpotifyToken();
        const url = `https://api.spotify.com/v1/search?q=${encodeURIComponent(query)}&type=track,artist&limit=7&market=IN`;
        const response = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
        if (response.ok) {
            const data = await response.json();
            const trackSuggestions = (data.tracks?.items || []).map((t) => `${t.name} - ${t.artists?.[0]?.name}`);
            const artistSuggestions = (data.artists?.items || []).map((a) => a.name);
            return res.json({ suggestions: Array.from(new Set([...trackSuggestions, ...artistSuggestions])).slice(0, 7) });
        }
    }
    catch { }
    return res.json({ suggestions: [] });
});
// 2. Search Tracks (Pure JS YouTube fallback guaranteed)
app.get('/api/search', async (req, res) => {
    const rawQuery = (req.query.q || '').trim();
    if (!rawQuery)
        return res.status(400).json({ error: 'Missing query parameter q' });
    const cleanQuery = rawQuery.replace(/["']/g, '').replace(/\s*-\s*/g, ' ').replace(/[()]/g, '').trim();
    let tracks = [];
    // Try Spotify first if credentials exist
    if (process.env.SPOTIFY_CLIENT_ID && process.env.SPOTIFY_CLIENT_SECRET) {
        try {
            const token = await getSpotifyToken();
            const spotifyUrl = `https://api.spotify.com/v1/search?q=${encodeURIComponent(cleanQuery)}&type=track&limit=15&market=IN`;
            const response = await fetch(spotifyUrl, { headers: { Authorization: `Bearer ${token}` } });
            if (response.ok) {
                const data = await response.json();
                if (data.tracks?.items?.length) {
                    tracks = data.tracks.items.map((t) => ({
                        id: t.id,
                        title: cleanTitle(t.name, cleanQuery),
                        artist: cleanArtist(t.artists),
                        movieOrAlbum: cleanTitle(t.album?.name, 'Single'),
                        artworkUrl: t.album?.images?.[0]?.url || t.album?.images?.[1]?.url || '',
                        duration: Math.floor((t.duration_ms || 0) / 1000),
                        releaseDate: t.album?.release_date || 'Recent',
                        source: 'spotify',
                    }));
                }
            }
        }
        catch (err) {
            console.warn('[SEARCH] Spotify error:', err.message);
        }
    }
    // Pure JavaScript YouTube Music search fallback (handles all songs, regional tracks & indie)
    if (tracks.length === 0) {
        try {
            const youtube = await getYouTube();
            const musicSearch = await youtube.music.search(cleanQuery, { type: 'song' });
            let songItems = [];
            // Unpack actual songs from shelf instead of using the shelf container
            if (musicSearch.songs?.contents?.length) {
                songItems = musicSearch.songs.contents;
            }
            else if (Array.isArray(musicSearch.contents)) {
                for (const item of musicSearch.contents) {
                    if (item.contents && Array.isArray(item.contents)) {
                        songItems.push(...item.contents);
                    }
                    else if (item.items && Array.isArray(item.items)) {
                        songItems.push(...item.items);
                    }
                    else if (item.id && item.type !== 'MusicShelf') {
                        songItems.push(item);
                    }
                }
            }
            // Filter out shelf objects and any item without a valid id
            songItems = songItems.filter((item) => item && item.id && item.type !== 'MusicShelf');
            if (songItems.length > 0) {
                tracks = songItems.map((song) => ({
                    id: String(song.id),
                    title: cleanTitle(song.title, cleanQuery),
                    artist: cleanArtist(song.artists || song.author),
                    movieOrAlbum: cleanTitle(song.album?.name || song.album, 'Single'),
                    artworkUrl: cleanArtwork(song.thumbnails),
                    duration: typeof song.duration === 'number' ? song.duration : (song.duration?.seconds || 0),
                    releaseDate: 'Recent',
                    source: 'youtube',
                })).filter((t) => t.id && t.title !== 'Songs');
            }
            // Fallback to general YouTube video search if Music search returned no tracks
            if (tracks.length === 0) {
                const generalSearch = await youtube.search(`${cleanQuery} song`, { type: 'video' });
                const videos = (generalSearch.videos || []).filter((v) => v && v.id);
                tracks = videos.map((v) => ({
                    id: String(v.id),
                    title: cleanTitle(v.title, cleanQuery),
                    artist: cleanArtist(v.author),
                    movieOrAlbum: 'YouTube Audio',
                    artworkUrl: cleanArtwork(v.thumbnails),
                    duration: typeof v.duration === 'number' ? v.duration : (v.duration?.seconds || 0),
                    releaseDate: 'Recent',
                    source: 'youtube',
                })).filter((t) => t.id && t.title !== 'Songs');
            }
        }
        catch (ytErr) {
            console.error('[SEARCH] YouTube fallback failed:', ytErr.message);
        }
    }
    console.log(`[SEARCH] Query: "${cleanQuery}" | Results found: ${tracks.length}`);
    return res.json({ count: tracks.length, results: tracks });
});
// Resilient Pure JavaScript Audio URL Resolver (Zero external binaries)
// 1. Innertube IOS client emulation (unciphered direct CDN URL)
// 2. Innertube standard info decipher
// 3. @distube/ytdl-core in-memory pure JS solver
async function resolveDirectAudioUrl(videoId) {
    const cached = streamCache.get(videoId);
    if (cached && Date.now() < cached.expiresAt) {
        return cached.url;
    }
    const youtube = await getYouTube();
    // Attempt 1: Innertube iOS client context (returns direct unciphered CDN audio URL without bot detection)
    try {
        const info = await youtube.getBasicInfo(videoId, { client: 'IOS' });
        const format = info.chooseFormat({ type: 'audio', quality: 'best' });
        if (format?.url) {
            streamCache.set(videoId, {
                url: format.url,
                expiresAt: Date.now() + 3 * 60 * 60 * 1000,
            });
            return format.url;
        }
    }
    catch (iosErr) {
        console.warn(`[STREAM] IOS client attempt for ${videoId}:`, iosErr?.message || iosErr);
    }
    // Attempt 2: Innertube standard getInfo & decipher
    try {
        const info = await youtube.getInfo(videoId);
        const format = info.chooseFormat({ type: 'audio', quality: 'best' });
        if (format?.url) {
            streamCache.set(videoId, {
                url: format.url,
                expiresAt: Date.now() + 3 * 60 * 60 * 1000,
            });
            return format.url;
        }
        if (format) {
            const deciphered = await format.decipher(youtube.session.player);
            if (deciphered) {
                streamCache.set(videoId, {
                    url: deciphered,
                    expiresAt: Date.now() + 3 * 60 * 60 * 1000,
                });
                return deciphered;
            }
        }
    }
    catch (decErr) {
        console.warn(`[STREAM] Standard decipher attempt for ${videoId}:`, decErr?.message || decErr);
    }
    // Attempt 3: Pure JS in-memory solver via @distube/ytdl-core (no external binaries)
    try {
        const info = await ytdl.getInfo(videoId);
        const format = info.formats.find((f) => f.hasAudio && !f.hasVideo && f.url);
        if (format?.url) {
            streamCache.set(videoId, {
                url: format.url,
                expiresAt: Date.now() + 3 * 60 * 60 * 1000,
            });
            return format.url;
        }
    }
    catch (ytdlErr) {
        console.warn(`[STREAM] ytdl-core in-memory attempt for ${videoId}:`, ytdlErr?.message || ytdlErr);
    }
    throw new Error(`Unable to resolve playable audio URL for videoId: ${videoId}`);
}
// 3. Audio Streaming Pipe / Redirect Endpoint
app.get('/api/stream', async (req, res) => {
    const videoId = (req.query.id || req.query.videoId || '').trim();
    if (!videoId) {
        return res.status(400).json({ error: 'Video ID required (?id=<videoId> or ?videoId=<videoId>)' });
    }
    try {
        // If redirect requested
        if (req.query.redirect === 'true') {
            const streamUrl = await resolveDirectAudioUrl(videoId);
            return res.redirect(302, streamUrl);
        }
        // Try resolving direct URL and piping with Range headers
        let streamUrl;
        try {
            streamUrl = await resolveDirectAudioUrl(videoId);
        }
        catch (urlErr) {
            console.warn(`[STREAM] Direct URL resolution failed for pipe ${videoId}:`, urlErr);
        }
        if (streamUrl) {
            const rangeHeader = req.headers.range;
            const fetchHeaders = {};
            if (rangeHeader) {
                fetchHeaders['Range'] = rangeHeader;
            }
            const upstreamRes = await fetch(streamUrl, { headers: fetchHeaders });
            if (upstreamRes.ok || upstreamRes.status === 206) {
                res.status(upstreamRes.status);
                const contentType = upstreamRes.headers.get('content-type') || 'audio/mp4';
                const contentLength = upstreamRes.headers.get('content-length');
                const contentRange = upstreamRes.headers.get('content-range');
                const acceptRanges = upstreamRes.headers.get('accept-ranges') || 'bytes';
                res.setHeader('Content-Type', contentType);
                res.setHeader('Accept-Ranges', acceptRanges);
                if (contentLength)
                    res.setHeader('Content-Length', contentLength);
                if (contentRange)
                    res.setHeader('Content-Range', contentRange);
                if (upstreamRes.body) {
                    const nodeStream = Readable.fromWeb(upstreamRes.body);
                    nodeStream.pipe(res);
                    nodeStream.on('error', (pipeErr) => {
                        console.error('[STREAM FATAL ERROR]: Pipe streaming error:', pipeErr);
                    });
                    return;
                }
            }
            // If range request pipe failed, redirect directly to YouTube CDN
            return res.redirect(302, streamUrl);
        }
        // Fallback: pipe directly via youtube.download
        const youtube = await getYouTube();
        const downloadStream = await youtube.download(videoId, { type: 'audio', quality: 'best' });
        res.setHeader('Content-Type', 'audio/webm');
        const nodeStream = Readable.fromWeb(downloadStream);
        nodeStream.pipe(res);
        nodeStream.on('error', (err) => {
            console.error('[STREAM FATAL ERROR]: youtube.download pipe error:', err);
        });
    }
    catch (err) {
        console.error('[STREAM FATAL ERROR]:', err);
        return res.status(500).json({
            error: 'Stream failed',
            message: err?.message || 'Unknown stream fatal error',
        });
    }
});
// 4. Track Stream URL Resolver
app.get('/api/resolve', async (req, res) => {
    const title = (req.query.title || '').trim();
    const artist = (req.query.artist || '').trim();
    if (!title)
        return res.status(400).json({ error: 'Title required' });
    try {
        const query = `${title} ${artist} audio`;
        let videoId;
        const youtube = await getYouTube();
        try {
            const musicSearch = await youtube.music.search(query, { type: 'song' });
            if (musicSearch.songs?.contents?.[0]?.id) {
                videoId = musicSearch.songs.contents[0].id;
            }
            else if (Array.isArray(musicSearch.contents)) {
                for (const item of musicSearch.contents) {
                    if (item.contents?.[0]?.id) {
                        videoId = item.contents[0].id;
                        break;
                    }
                    else if (item.items?.[0]?.id) {
                        videoId = item.items[0].id;
                        break;
                    }
                    else if (item.id && item.type !== 'MusicShelf') {
                        videoId = item.id;
                        break;
                    }
                }
            }
        }
        catch (searchErr) {
            console.warn('[RESOLVE] YouTube Music search warning:', searchErr?.message || searchErr);
        }
        if (!videoId) {
            try {
                const generalSearch = await youtube.search(query, { type: 'video' });
                videoId = generalSearch.videos?.[0]?.id;
            }
            catch (generalErr) {
                console.warn('[RESOLVE] YouTube video search warning:', generalErr?.message || generalErr);
            }
        }
        if (!videoId)
            return res.status(404).json({ error: 'Track not found' });
        let directUrl;
        try {
            directUrl = await resolveDirectAudioUrl(videoId);
        }
        catch (resolveErr) {
            console.warn(`[RESOLVE] Direct audio URL resolution warning for ${videoId}:`, resolveErr);
        }
        return res.json({
            streamUrl: directUrl || `/api/stream?id=${videoId}`,
            proxyStreamUrl: `/api/stream?id=${videoId}`,
            directUrl: directUrl || null,
            videoId,
        });
    }
    catch (err) {
        console.error('[STREAM FATAL ERROR]:', err);
        return res.status(500).json({
            error: 'Resolve failed',
            message: err?.message || 'Unknown resolve error',
        });
    }
});
// 5. Synced Lyrics
app.get('/api/lyrics', async (req, res) => {
    const title = (req.query.title || '').trim();
    const artist = (req.query.artist || '').trim();
    const duration = req.query.duration ? Number(req.query.duration) : undefined;
    if (!title)
        return res.status(400).json({ error: 'Title required' });
    try {
        let url = `https://lrclib.net/api/get?track_name=${encodeURIComponent(title)}&artist_name=${encodeURIComponent(artist)}`;
        if (duration)
            url += `&duration=${duration}`;
        let response = await fetch(url, { headers: { 'User-Agent': 'Spotless/1.0' } });
        if (!response.ok) {
            const searchUrl = `https://lrclib.net/api/search?q=${encodeURIComponent(`${title} ${artist}`)}`;
            const searchRes = await fetch(searchUrl, { headers: { 'User-Agent': 'Spotless/1.0' } });
            const searchData = await searchRes.json();
            if (Array.isArray(searchData) && searchData.length > 0) {
                return res.json({
                    syncedLyrics: searchData[0].syncedLyrics || null,
                    plainLyrics: searchData[0].plainLyrics || null,
                });
            }
            return res.status(404).json({ error: 'Lyrics not found' });
        }
        const data = await response.json();
        return res.json({
            syncedLyrics: data.syncedLyrics || null,
            plainLyrics: data.plainLyrics || null,
        });
    }
    catch {
        return res.status(500).json({ error: 'Failed to fetch lyrics' });
    }
});
// 6. Automated Playlist Recommendations
app.get('/api/recommendations', async (req, res) => {
    let tracks = [];
    // Attempt 1: Spotify Recommendations (if available)
    if (process.env.SPOTIFY_CLIENT_ID && process.env.SPOTIFY_CLIENT_SECRET) {
        try {
            const token = await getSpotifyToken();
            const candidateTracks = [...recentlyPlayed, ...likedSongs];
            const spotifySeedIds = candidateTracks
                .filter((t) => t.source === 'spotify' || (t.id && !t.id.startsWith('pl_') && t.id.length === 22))
                .map((t) => t.id)
                .slice(0, 5);
            const recUrl = spotifySeedIds.length > 0
                ? `https://api.spotify.com/v1/recommendations?limit=20&seed_tracks=${spotifySeedIds.join(',')}&market=IN`
                : `https://api.spotify.com/v1/recommendations?limit=20&seed_genres=pop,indie,hip-hop&market=IN`;
            const response = await fetch(recUrl, { headers: { Authorization: `Bearer ${token}` } });
            if (response.ok) {
                const data = await response.json();
                if (Array.isArray(data.tracks) && data.tracks.length > 0) {
                    tracks = data.tracks.map((t) => ({
                        id: t.id,
                        title: cleanTitle(t.name),
                        artist: cleanArtist(t.artists),
                        movieOrAlbum: cleanTitle(t.album?.name, 'Single'),
                        artworkUrl: t.album?.images?.[0]?.url || t.album?.images?.[1]?.url || '',
                        duration: Math.floor((t.duration_ms || 0) / 1000),
                        releaseDate: t.album?.release_date || 'Recent',
                        source: 'spotify',
                    }));
                }
            }
        }
        catch (err) {
            console.warn('[RECOMMENDATIONS] Spotify failed, falling back:', err.message);
        }
    }
    // Attempt 2: YouTube Music recommendations based on listening history or trending hits
    if (tracks.length === 0) {
        try {
            const youtube = await getYouTube();
            // Pick seed query based on recent history or fallback to top hits
            const lastTrack = recentlyPlayed[0] || likedSongs[0];
            const seedQuery = lastTrack?.artist ? `${lastTrack.artist} top songs` : 'top hits';
            const musicSearch = await youtube.music.search(seedQuery, { type: 'song' });
            let songItems = [];
            if (musicSearch.songs?.contents?.length) {
                songItems = musicSearch.songs.contents;
            }
            else if (Array.isArray(musicSearch.contents)) {
                for (const item of musicSearch.contents) {
                    if (item.contents && Array.isArray(item.contents))
                        songItems.push(...item.contents);
                    else if (item.items && Array.isArray(item.items))
                        songItems.push(...item.items);
                    else if (item.id && item.type !== 'MusicShelf')
                        songItems.push(item);
                }
            }
            songItems = songItems.filter((item) => item && item.id && item.type !== 'MusicShelf');
            if (songItems.length > 0) {
                tracks = songItems.map((song) => ({
                    id: String(song.id),
                    title: cleanTitle(song.title, 'Track'),
                    artist: cleanArtist(song.artists || song.author),
                    movieOrAlbum: cleanTitle(song.album?.name || song.album, 'Single'),
                    artworkUrl: cleanArtwork(song.thumbnails),
                    duration: typeof song.duration === 'number' ? song.duration : (song.duration?.seconds || 0),
                    releaseDate: 'Recent',
                    source: 'youtube',
                })).filter((t) => t.id && t.title !== 'Songs');
            }
        }
        catch (ytErr) {
            console.warn('[RECOMMENDATIONS] YouTube fallback error:', ytErr.message);
        }
    }
    // Attempt 3: Guaranteed curated fallback tracks (safe default)
    if (tracks.length === 0) {
        tracks = [
            {
                id: '3_g2un5M350',
                title: 'Starboy',
                artist: 'The Weeknd, Daft Punk',
                movieOrAlbum: 'Starboy',
                artworkUrl: 'https://i.scdn.co/image/ab67616d0000b2734718e2b124f79258be7bc452',
                duration: 230,
                releaseDate: '2016-11-25',
                source: 'curated',
            },
            {
                id: 'J7p4bzqLvCw',
                title: 'Blinding Lights',
                artist: 'The Weeknd',
                movieOrAlbum: 'After Hours',
                artworkUrl: 'https://i.scdn.co/image/ab67616d0000b2738863bc11d2aa12b54f5aeb36',
                duration: 200,
                releaseDate: '2020-03-20',
                source: 'curated',
            },
            {
                id: 'OsfAnsMY21M',
                title: 'Levitating',
                artist: 'Dua Lipa',
                movieOrAlbum: 'Future Nostalgia',
                artworkUrl: 'https://i.scdn.co/image/ab67616d0000b273bd26ede1ae69327010d49946',
                duration: 203,
                releaseDate: '2020-03-27',
                source: 'curated',
            },
            {
                id: 'nujn6wbr-e8',
                title: 'As It Was',
                artist: 'Harry Styles',
                movieOrAlbum: "Harry's House",
                artworkUrl: 'https://i.scdn.co/image/ab67616d0000b2732e8ed79e177ff6011076f5f0',
                duration: 167,
                releaseDate: '2022-05-20',
                source: 'curated',
            },
        ];
    }
    return res.json({
        playlistName: 'Made For You',
        description: 'Dynamic recommendations based on your listening',
        tracks,
    });
});
// 7. Listening History
app.get('/api/history', (_req, res) => {
    return res.json({ history: recentlyPlayed });
});
app.post('/api/history', (req, res) => {
    const track = req.body.track;
    if (!track || !track.id)
        return res.status(400).json({ error: 'Invalid track' });
    const sanitizedTrack = {
        ...track,
        title: cleanTitle(track.title),
        artist: cleanArtist(track.artist),
        movieOrAlbum: cleanTitle(track.movieOrAlbum, 'Single'),
        artworkUrl: typeof track.artworkUrl === 'string' ? track.artworkUrl : '',
    };
    recentlyPlayed = [sanitizedTrack, ...recentlyPlayed.filter((t) => t.id !== track.id)].slice(0, 50);
    return res.json({ success: true, history: recentlyPlayed });
});
// 8. Liked Tracks
app.get('/api/liked', (_req, res) => {
    return res.json({ count: likedSongs.length, tracks: likedSongs });
});
app.post('/api/liked/toggle', (req, res) => {
    const track = req.body.track;
    if (!track || !track.id)
        return res.status(400).json({ error: 'Invalid track' });
    const sanitizedTrack = {
        ...track,
        title: cleanTitle(track.title),
        artist: cleanArtist(track.artist),
        movieOrAlbum: cleanTitle(track.movieOrAlbum, 'Single'),
        artworkUrl: typeof track.artworkUrl === 'string' ? track.artworkUrl : '',
    };
    const existsIdx = likedSongs.findIndex((t) => t.id === track.id);
    let isLiked = false;
    if (existsIdx > -1) {
        likedSongs.splice(existsIdx, 1);
    }
    else {
        likedSongs.unshift(sanitizedTrack);
        isLiked = true;
    }
    return res.json({ isLiked, count: likedSongs.length, tracks: likedSongs });
});
// 9. Playlists
app.get('/api/playlists', (_req, res) => {
    return res.json({ playlists });
});
app.post('/api/playlists', (req, res) => {
    const { name, description, isPublic } = req.body;
    if (!name)
        return res.status(400).json({ error: 'Playlist name required' });
    const newPlaylist = {
        id: `pl_${Date.now()}`,
        name: String(name),
        description: description ? String(description) : '',
        isPublic: Boolean(isPublic),
        tracks: [],
        createdAt: new Date().toISOString(),
    };
    playlists.unshift(newPlaylist);
    return res.status(201).json({ playlist: newPlaylist });
});
app.listen(Number(PORT), '0.0.0.0', () => {
    console.log(`Backend server active on http://0.0.0.0:${PORT}`);
});
//# sourceMappingURL=index.js.map