import express from 'express';
import type { Request, Response } from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { Innertube } from 'youtubei.js';
import { Ytdl } from '@distube/ytdl-core';
import { spawn } from 'child_process';
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

interface Track {
  id: string;
  title: string;
  artist: string;
  artworkUrl?: string;
  duration?: number;
  movieOrAlbum?: string;
  releaseDate?: string;
  source?: string;
}

interface Playlist {
  id: string;
  name: string;
  description: string;
  isPublic: boolean;
  tracks: Track[];
  createdAt: string;
}

const streamCache = new Map<string, { url: string; expiresAt: number }>();
let likedSongs: Track[] = [];
let recentlyPlayed: Track[] = [];
let playlists: Playlist[] = [];

let yt: Innertube | null = null;
(async () => {
  try {
    yt = await Innertube.create();
    console.log('⚡ Fast Search & Audio Engine ready');
  } catch (err) {
    console.error('Innertube search init error:', err);
  }
})();

// Resilient @distube/ytdl-core client configuration using multi-client InnerTube fallback
// (visionos, web_embedded, tv, mweb bypass YouTube data-center bot detection blocks)
const ytdl = new Ytdl({
  clients: ['visionos', 'web_embedded', 'tv', 'mweb'],
});

// Sanitization Helpers to guarantee primitive strings are returned
function cleanTitle(val: any, fallback = 'Unknown'): string {
  if (!val) return fallback;
  if (typeof val === 'string') return val.trim() || fallback;
  if (typeof val?.text === 'string') return val.text.trim() || fallback;
  if (Array.isArray(val?.runs) && val.runs.length > 0) {
    const text = val.runs.map((r: any) => r.text || '').join('').trim();
    if (text) return text;
  }
  if (typeof val?.name === 'string') return val.name.trim() || fallback;
  if (typeof val?.toString === 'function') {
    const str = val.toString();
    if (str && str !== '[object Object]') return str.trim();
  }
  return fallback;
}

function cleanArtist(val: any, fallback = 'Artist'): string {
  if (!val) return fallback;
  if (typeof val === 'string') return val.trim() || fallback;
  if (Array.isArray(val)) {
    const names = val.map((item: any) => {
      if (typeof item === 'string') return item.trim();
      return item?.name?.trim?.() || item?.text?.trim?.() || '';
    }).filter(Boolean);
    if (names.length > 0) return names.join(', ');
  }
  if (typeof val?.name === 'string') return val.name.trim() || fallback;
  if (typeof val?.text === 'string') return val.text.trim() || fallback;
  if (Array.isArray(val?.runs) && val.runs.length > 0) {
    const text = val.runs.map((r: any) => r.text || '').join('').trim();
    if (text) return text;
  }
  return fallback;
}

function cleanArtwork(val: any): string {
  if (!val) return '';
  if (typeof val === 'string') return val;
  if (Array.isArray(val) && val.length > 0) {
    const last = val[val.length - 1];
    return last?.url || val[0]?.url || '';
  }
  return val.url || '';
}

let spotifyToken = '';
let tokenExpiresAt = 0;

async function getSpotifyToken(): Promise<string> {
  if (spotifyToken && Date.now() < tokenExpiresAt) return spotifyToken;
  const clientId = process.env.SPOTIFY_CLIENT_ID;
  const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;
  if (!clientId || !clientSecret) throw new Error('Missing Spotify credentials in .env');

  const res = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`,
    },
    body: 'grant_type=client_credentials',
  });
  if (!res.ok) throw new Error(`Spotify token error: ${res.status}`);
  const data: any = await res.json();
  spotifyToken = data.access_token;
  tokenExpiresAt = Date.now() + (data.expires_in - 300) * 1000;
  return spotifyToken;
}

// 1. Search Suggestions
app.get('/api/suggestions', async (req: Request, res: Response) => {
  const query = (req.query.q as string || '').trim();
  if (!query) return res.json({ suggestions: [] });

  try {
    const token = await getSpotifyToken();
    const url = `https://api.spotify.com/v1/search?q=${encodeURIComponent(query)}&type=track,artist&limit=7&market=IN`;
    const response = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (response.ok) {
      const data: any = await response.json();
      const trackSuggestions = (data.tracks?.items || []).map((t: any) => `${t.name} - ${t.artists?.[0]?.name}`);
      const artistSuggestions = (data.artists?.items || []).map((a: any) => a.name);
      return res.json({ suggestions: Array.from(new Set([...trackSuggestions, ...artistSuggestions])).slice(0, 7) });
    }
  } catch {}

  return res.json({ suggestions: [] });
});

// 2. Search Tracks (Robust YouTube fallback guaranteed)
app.get('/api/search', async (req: Request, res: Response) => {
  const rawQuery = (req.query.q as string || '').trim();
  if (!rawQuery) return res.status(400).json({ error: 'Missing query parameter q' });

  const cleanQuery = rawQuery.replace(/["']/g, '').replace(/\s*-\s*/g, ' ').replace(/[()]/g, '').trim();
  let tracks: Track[] = [];

  // Try Spotify first if credentials exist
  if (process.env.SPOTIFY_CLIENT_ID && process.env.SPOTIFY_CLIENT_SECRET) {
    try {
      const token = await getSpotifyToken();
      const spotifyUrl = `https://api.spotify.com/v1/search?q=${encodeURIComponent(cleanQuery)}&type=track&limit=15&market=IN`;
      const response = await fetch(spotifyUrl, { headers: { Authorization: `Bearer ${token}` } });

      if (response.ok) {
        const data: any = await response.json();
        if (data.tracks?.items?.length) {
          tracks = data.tracks.items.map((t: any) => ({
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
    } catch (err: any) {
      console.warn('[SEARCH] Spotify error:', err.message);
    }
  }

  // YouTube Music search fallback (handles all songs, regional tracks & indie)
  if (tracks.length === 0) {
    try {
      if (!yt) {
        yt = await Innertube.create();
      }

      const musicSearch: any = await yt.music.search(cleanQuery, { type: 'song' });
      let songItems: any[] = [];

      // Unpack actual songs from shelf instead of using the shelf container
      if (musicSearch.songs?.contents?.length) {
        songItems = musicSearch.songs.contents;
      } else if (Array.isArray(musicSearch.contents)) {
        for (const item of musicSearch.contents) {
          if (item.contents && Array.isArray(item.contents)) {
            songItems.push(...item.contents);
          } else if (item.items && Array.isArray(item.items)) {
            songItems.push(...item.items);
          } else if (item.id && item.type !== 'MusicShelf') {
            songItems.push(item);
          }
        }
      }

      // Filter out shelf objects and any item without a valid id
      songItems = songItems.filter((item: any) => item && item.id && item.type !== 'MusicShelf');

      if (songItems.length > 0) {
        tracks = songItems.map((song: any) => ({
          id: String(song.id),
          title: cleanTitle(song.title, cleanQuery),
          artist: cleanArtist(song.artists || song.author),
          movieOrAlbum: cleanTitle(song.album?.name || song.album, 'Single'),
          artworkUrl: cleanArtwork(song.thumbnails),
          duration: typeof song.duration === 'number' ? song.duration : (song.duration?.seconds || 0),
          releaseDate: 'Recent',
          source: 'youtube',
        })).filter((t: any) => t.id && t.title !== 'Songs');
      }

      // Fallback to general YouTube video search if Music search returned no tracks
      if (tracks.length === 0) {
        const generalSearch: any = await yt.search(`${cleanQuery} song`, { type: 'video' });
        const videos = (generalSearch.videos || []).filter((v: any) => v && v.id);
        tracks = videos.map((v: any) => ({
          id: String(v.id),
          title: cleanTitle(v.title, cleanQuery),
          artist: cleanArtist(v.author),
          movieOrAlbum: 'YouTube Audio',
          artworkUrl: cleanArtwork(v.thumbnails),
          duration: typeof v.duration === 'number' ? v.duration : (v.duration?.seconds || 0),
          releaseDate: 'Recent',
          source: 'youtube',
        })).filter((t: any) => t.id && t.title !== 'Songs');
      }
    } catch (ytErr: any) {
      console.error('[SEARCH] YouTube fallback failed:', ytErr.message);
    }
  }

  console.log(`[SEARCH] Query: "${cleanQuery}" | Results found: ${tracks.length}`);
  return res.json({ count: tracks.length, results: tracks });
});

// Secondary fallback: yt-dlp if installed on host
function resolveWithYtDlp(videoId: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const proc = spawn('yt-dlp', [
      '-g',
      '-f', '140/ba',
      '--no-playlist',
      '--skip-download',
      '--no-warnings',
      '--no-check-certificates',
      `https://www.youtube.com/watch?v=${videoId}`,
    ]);

    let output = '';
    let errOutput = '';

    proc.stdout.on('data', (d) => { output += d.toString(); });
    proc.stderr.on('data', (d) => { errOutput += d.toString(); });

    proc.on('close', (code) => {
      const streamUrl = output.trim().split('\n')[0];
      if (code === 0 && streamUrl) {
        resolve(streamUrl);
      } else {
        reject(new Error(errOutput || `yt-dlp exit code ${code}`));
      }
    });

    proc.on('error', (err) => reject(err));
  });
}

// Resilient Audio URL resolver:
// 1. @distube/ytdl-core using InnerTube client emulation (visionos, web_embedded, tv, mweb)
// 2. yt-dlp binary as secondary host fallback
async function resolveDirectAudioUrl(videoId: string): Promise<string> {
  const cached = streamCache.get(videoId);
  if (cached && Date.now() < cached.expiresAt) {
    return cached.url;
  }

  // 1. Primary: @distube/ytdl-core with resilient InnerTube clients
  try {
    const info = await ytdl.getInfo(videoId);
    const audioFormats = info.formats
      .filter((f) => f.hasAudio && !f.hasVideo && f.url)
      .sort((a, b) => (b.bitrate || 0) - (a.bitrate || 0));

    if (audioFormats.length > 0 && audioFormats[0]?.url) {
      const directUrl = audioFormats[0].url;
      streamCache.set(videoId, {
        url: directUrl,
        expiresAt: Date.now() + 3 * 60 * 60 * 1000,
      });
      return directUrl;
    }
  } catch (err: any) {
    console.warn(`[STREAM] @distube/ytdl-core extraction attempt failed for ${videoId}:`, err?.message || err);
  }

  // 2. Secondary fallback: yt-dlp if installed on host
  try {
    const directUrl = await resolveWithYtDlp(videoId);
    if (directUrl) {
      streamCache.set(videoId, {
        url: directUrl,
        expiresAt: Date.now() + 3 * 60 * 60 * 1000,
      });
      return directUrl;
    }
  } catch (dlpErr: any) {
    console.warn(`[STREAM] yt-dlp fallback failed for ${videoId}:`, dlpErr?.message || dlpErr);
  }

  throw new Error(`Could not extract playable audio stream for video ID: ${videoId}`);
}

// 3. Audio Streaming Pipe Endpoint
// Pipes deciphered audio bytes with HTTP 206 Partial Content Range support
app.get('/api/stream', async (req: Request, res: Response) => {
  const videoId = ((req.query.id as string) || (req.query.videoId as string) || '').trim();
  if (!videoId) {
    return res.status(400).json({ error: 'Video ID required (?id=<videoId> or ?videoId=<videoId>)' });
  }

  try {
    const directUrl = await resolveDirectAudioUrl(videoId);

    // If client explicitly requested a 302 redirect
    if (req.query.redirect === 'true') {
      return res.redirect(directUrl);
    }

    // Proxy stream with Range request handling
    const rangeHeader = req.headers.range;
    const fetchHeaders: Record<string, string> = {};
    if (rangeHeader) {
      fetchHeaders['Range'] = rangeHeader;
    }

    const upstreamRes = await fetch(directUrl, { headers: fetchHeaders });

    if (!upstreamRes.ok && upstreamRes.status !== 206) {
      // Fallback: redirect if upstream proxy request rejected
      return res.redirect(directUrl);
    }

    res.status(upstreamRes.status);
    const contentType = upstreamRes.headers.get('content-type') || 'audio/mp4';
    const contentLength = upstreamRes.headers.get('content-length');
    const contentRange = upstreamRes.headers.get('content-range');
    const acceptRanges = upstreamRes.headers.get('accept-ranges') || 'bytes';

    res.setHeader('Content-Type', contentType);
    res.setHeader('Accept-Ranges', acceptRanges);
    if (contentLength) res.setHeader('Content-Length', contentLength);
    if (contentRange) res.setHeader('Content-Range', contentRange);

    if (upstreamRes.body) {
      const nodeStream = Readable.fromWeb(upstreamRes.body as any);
      nodeStream.pipe(res);
      nodeStream.on('error', (pipeErr) => {
        console.error('[STREAM ERROR] Pipe streaming error:', pipeErr);
      });
    } else {
      res.end();
    }
  } catch (err: any) {
    console.error('[STREAM ERROR]', err);
    return res.status(500).json({
      error: 'Stream failed',
      message: err?.message || 'Unknown stream error',
    });
  }
});

// 4. Track Stream URL Resolver
app.get('/api/resolve', async (req: Request, res: Response) => {
  const title = ((req.query.title as string) || '').trim();
  const artist = ((req.query.artist as string) || '').trim();

  if (!title) return res.status(400).json({ error: 'Title required' });

  try {
    const query = `${title} ${artist} audio`;
    let videoId: string | undefined;

    if (yt) {
      try {
        const musicSearch: any = await yt.music.search(query, { type: 'song' });
        if (musicSearch.songs?.contents?.[0]?.id) {
          videoId = musicSearch.songs.contents[0].id;
        } else if (Array.isArray(musicSearch.contents)) {
          for (const item of musicSearch.contents) {
            if (item.contents?.[0]?.id) {
              videoId = item.contents[0].id;
              break;
            } else if (item.items?.[0]?.id) {
              videoId = item.items[0].id;
              break;
            } else if (item.id && item.type !== 'MusicShelf') {
              videoId = item.id;
              break;
            }
          }
        }
      } catch (searchErr: any) {
        console.warn('[RESOLVE] YouTube Music search warning:', searchErr?.message || searchErr);
      }

      if (!videoId) {
        try {
          const generalSearch: any = await yt.search(query, { type: 'video' });
          videoId = generalSearch.videos?.[0]?.id;
        } catch (generalErr: any) {
          console.warn('[RESOLVE] YouTube video search warning:', generalErr?.message || generalErr);
        }
      }
    }

    if (!videoId) return res.status(404).json({ error: 'Track not found' });

    const directUrl = await resolveDirectAudioUrl(videoId);
    return res.json({
      streamUrl: directUrl,
      proxyStreamUrl: `/api/stream?id=${videoId}`,
      directUrl,
      videoId,
    });
  } catch (err: any) {
    console.error('[STREAM ERROR]', err);
    return res.status(500).json({
      error: 'Resolve failed',
      message: err?.message || 'Unknown resolve error',
    });
  }
});

// 5. Synced Lyrics
app.get('/api/lyrics', async (req: Request, res: Response) => {
  const title = (req.query.title as string || '').trim();
  const artist = (req.query.artist as string || '').trim();
  const duration = req.query.duration ? Number(req.query.duration) : undefined;

  if (!title) return res.status(400).json({ error: 'Title required' });

  try {
    let url = `https://lrclib.net/api/get?track_name=${encodeURIComponent(title)}&artist_name=${encodeURIComponent(artist)}`;
    if (duration) url += `&duration=${duration}`;

    let response = await fetch(url, { headers: { 'User-Agent': 'Spotless/1.0' } });

    if (!response.ok) {
      const searchUrl = `https://lrclib.net/api/search?q=${encodeURIComponent(`${title} ${artist}`)}`;
      const searchRes = await fetch(searchUrl, { headers: { 'User-Agent': 'Spotless/1.0' } });
      const searchData: any = await searchRes.json();

      if (Array.isArray(searchData) && searchData.length > 0) {
        return res.json({
          syncedLyrics: searchData[0].syncedLyrics || null,
          plainLyrics: searchData[0].plainLyrics || null,
        });
      }
      return res.status(404).json({ error: 'Lyrics not found' });
    }

    const data: any = await response.json();
    return res.json({
      syncedLyrics: data.syncedLyrics || null,
      plainLyrics: data.plainLyrics || null,
    });
  } catch {
    return res.status(500).json({ error: 'Failed to fetch lyrics' });
  }
});

// 6. Automated Playlist Recommendations
app.get('/api/recommendations', async (req: Request, res: Response) => {
  let tracks: Track[] = [];

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
        const data: any = await response.json();
        if (Array.isArray(data.tracks) && data.tracks.length > 0) {
          tracks = data.tracks.map((t: any) => ({
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
    } catch (err: any) {
      console.warn('[RECOMMENDATIONS] Spotify failed, falling back:', err.message);
    }
  }

  // Attempt 2: YouTube Music recommendations based on listening history or trending hits
  if (tracks.length === 0) {
    try {
      if (!yt) {
        yt = await Innertube.create();
      }

      // Pick seed query based on recent history or fallback to top hits
      const lastTrack = recentlyPlayed[0] || likedSongs[0];
      const seedQuery = lastTrack?.artist ? `${lastTrack.artist} top songs` : 'top hits';

      const musicSearch: any = await yt.music.search(seedQuery, { type: 'song' });
      let songItems: any[] = [];

      if (musicSearch.songs?.contents?.length) {
        songItems = musicSearch.songs.contents;
      } else if (Array.isArray(musicSearch.contents)) {
        for (const item of musicSearch.contents) {
          if (item.contents && Array.isArray(item.contents)) songItems.push(...item.contents);
          else if (item.items && Array.isArray(item.items)) songItems.push(...item.items);
          else if (item.id && item.type !== 'MusicShelf') songItems.push(item);
        }
      }

      songItems = songItems.filter((item: any) => item && item.id && item.type !== 'MusicShelf');

      if (songItems.length > 0) {
        tracks = songItems.map((song: any) => ({
          id: String(song.id),
          title: cleanTitle(song.title, 'Track'),
          artist: cleanArtist(song.artists || song.author),
          movieOrAlbum: cleanTitle(song.album?.name || song.album, 'Single'),
          artworkUrl: cleanArtwork(song.thumbnails),
          duration: typeof song.duration === 'number' ? song.duration : (song.duration?.seconds || 0),
          releaseDate: 'Recent',
          source: 'youtube',
        })).filter((t: any) => t.id && t.title !== 'Songs');
      }
    } catch (ytErr: any) {
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
        id: '7wfGh8n714w',
        title: 'Blinding Lights',
        artist: 'The Weeknd',
        movieOrAlbum: 'After Hours',
        artworkUrl: 'https://i.scdn.co/image/ab67616d0000b2738863bc11d2aa12b54f5aeb36',
        duration: 200,
        releaseDate: '2020-03-20',
        source: 'curated',
      },
      {
        id: '0VwhorTQe50',
        title: 'Levitating',
        artist: 'Dua Lipa',
        movieOrAlbum: 'Future Nostalgia',
        artworkUrl: 'https://i.scdn.co/image/ab67616d0000b273bd26ede1ae69327010d49946',
        duration: 203,
        releaseDate: '2020-03-27',
        source: 'curated',
      },
      {
        id: 'gNi_6U5Pm_o',
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
app.get('/api/history', (_req: Request, res: Response) => {
  return res.json({ history: recentlyPlayed });
});

app.post('/api/history', (req: Request, res: Response) => {
  const track: Track = req.body.track;
  if (!track || !track.id) return res.status(400).json({ error: 'Invalid track' });

  const sanitizedTrack: Track = {
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
app.get('/api/liked', (_req: Request, res: Response) => {
  return res.json({ count: likedSongs.length, tracks: likedSongs });
});

app.post('/api/liked/toggle', (req: Request, res: Response) => {
  const track: Track = req.body.track;
  if (!track || !track.id) return res.status(400).json({ error: 'Invalid track' });

  const sanitizedTrack: Track = {
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
  } else {
    likedSongs.unshift(sanitizedTrack);
    isLiked = true;
  }

  return res.json({ isLiked, count: likedSongs.length, tracks: likedSongs });
});

// 9. Playlists
app.get('/api/playlists', (_req: Request, res: Response) => {
  return res.json({ playlists });
});

app.post('/api/playlists', (req: Request, res: Response) => {
  const { name, description, isPublic } = req.body;
  if (!name) return res.status(400).json({ error: 'Playlist name required' });

  const newPlaylist: Playlist = {
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
