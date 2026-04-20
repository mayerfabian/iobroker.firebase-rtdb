# ioBroker Firebase RTDB Sync

MVP for an ioBroker adapter that writes selected ioBroker states to Firebase Realtime Database:

- `/home/current` contains the complete current snapshot.
- `/home/history/<YYYY-MM-DD>/<field-path>/<timestamp>` contains granular history values.
- Firebase Service Account credentials stay in the backend adapter only.
- The future Angular PWA reads with Firebase Web SDK only.

## Development

```powershell
npm install
npm run build
```

## Configuration

The MVP already reserves native adapter settings for:

- `databaseUrl`: Firebase RTDB URL, for example `https://project-id-default-rtdb.europe-west1.firebasedatabase.app`
- `rootPath`: Firebase root path, default `home`
- `serviceAccountJson`: encrypted ioBroker native field for the Service Account JSON
- `dryRun`: logs writes and updates debug states without sending anything to Firebase
- `dailyWriteHour` / `dailyWriteMinute`: time for daily-only fields
- `channels`: future UI override list for state IDs, thresholds and intervals

The adapter now has a dedicated admin tab `Firebase Sync` for channel management.
The instance configuration only keeps global Firebase and runtime settings.

## Debugging

Keep `dryRun` enabled while testing the adapter configuration. In dry-run mode the adapter runs the same runtime decisions, creates the same Firebase paths, and updates debug states, but it does not call Firebase.

Useful states:

- `info.connection`
- `info.lastWrite`
- `debug.lastTrigger`
- `debug.lastSkipped`
- `debug.lastWriteReason`
- `debug.lastWritePath`
- `debug.lastError`
- `debug.writeCount`
- `debug.skippedCount`

Set the adapter log level to `debug` in ioBroker to see skipped writes, delayed writes, deltas, triggers, and write reasons.

## Channel Management

- Add or edit Firebase channels in the adapter admin tab `Firebase Sync`
- The tab lists only states that are actually configured for this adapter
- Changes in the tab are stored as object custom settings and are applied after adapter restart
- The object custom icon remains available in `Objects` for per-state activation
