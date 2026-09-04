# Session Memory

## Preferences & Directives
- **Response Style**: Minimal, concise, code-only responses unless explicitly asked "why" or "how".
- **No Filler**: Skip all introductory and concluding conversational text, logs of edits, middle texts, thinking logs, and research summaries.
- **Diffs**: Do not print diffs in chat unless asked; list only the modified file paths.
- **Commands**: Do not run any `npm` or `npx` commands. Read-based commands (`grep`, `find`, etc.) and writing to files inside the project directory are permitted without confirmation.
- **Config**: Do not modify `.antigravity/instructions.md`.
- **Memory**: Keep adding conversation memory and details into `.antigravity/memory/`.

## Tasks & History
- Fixed PostgreSQL `operator does not exist: uuid = text` in `invalidate_missing_thumbnails.py` by casting array parameters to `%s::uuid[]` and ensuring stringified UUIDs in batches.
- Clarified that `invalidate_missing_thumbnails.py` does NOT touch face/person thumbnails (which reside in `person.thumbnailPath`), only asset thumbnails (`asset_file` / `asset`).
- Noted how to invalidate person thumbnails: set `person."thumbnailPath" = ''` for missing files on disk, then trigger "Generate Thumbnails" -> "Missing" in Immich Admin Jobs.
- Noted APIs for forcefully regenerating person/face thumbnails: PUT /api/people (with featureFaceAssetId) or POST /api/jobs / PUT /api/jobs/generate-thumbnails with force: true.
- Created `regenerate_face_thumbnails.py` to fetch all faces/people, invalidate `thumbnailPath` in Postgres (or via API), and trigger Immich thumbnail regeneration.
- Created `extract_face_thumbnails.py` to query `SELECT "thumbnailPath" FROM public.person`, preserve subpath after `upload/thumbs/`, and copy thumbnails to `./facesThumbnails`.
- Updated `refetch_missing_video_metadata.py` to inspect database (`asset_file` and `asset`) and API to verify availability of encoded video (`encoded_video` / `encodedVideoPath`) and thumbnail (`thumbnail`/`preview`/`thumbhash`). If either is missing or if technical metadata is missing, it queues the video for metadata refetch via `POST /api/assets/jobs` (`refresh-metadata`).
