(function () {
    const ADAPTER = 'firebase-history-sync';
    const DEFAULT_ROW = {
        sync: true,
        mode: 'threshold',
        minChange: 0,
        factor: 1,
        transform: 'none',
        timeUnit: 'ms',
        dailyHour: 0,
        dailyMinute: 10,
        round: 1,
        minSendIntervalMs: 10000,
        maxSendIntervalMs: 900000,
        defaultValue: ''
    };

    let socket;
    let instance = '0';
    let namespace = `${ADAPTER}.0`;
    let instanceObjectId = `system.adapter.${namespace}`;
    let rows = [];
    let removedStateIds = new Set();
    let initiallyLoadedStateIds = new Set();
    let availableStates = [];
    let loadedStateObjects = new Map();
    let pickerSelection = new Set();
    let pendingDeleteIndex = null;
    let skipDeleteConfirmForSession = false;
    let systemTimeIntervalId = null;
    let infoTooltipEl = null;
    let isSaving = false;
    const DEBUG_PREFIX = '[firebase-sync-admin]';
    const SOCKET_TIMEOUT_MS = 15000;

    window.registerSocketOnLoad(async function () {
        try {
            await initPage();
        } catch (error) {
            setStatus(`Fehler: ${error.message}`);
            renderRows('Fehler beim Initialisieren des Firebase-Sync-Tabs.');
            showToast(`Initialisierung fehlgeschlagen: ${error.message}`);
        }
    });

    async function initPage() {
        if (!window.io) {
            throw new Error('socket.io wurde nicht geladen');
        }

        debugLog('initPage start');
        socket = window.io.connect();
        instance = detectInstance();
        namespace = `${ADAPTER}.${instance}`;
        instanceObjectId = `system.adapter.${namespace}`;
        debugLog('detected instance', { instance, namespace, instanceObjectId });

        bindUi();
        startSystemTimeTicker();
        setStatus(`Instanz ${namespace}`);
        await loadPage();
    }

    function bindUi() {
        $('#reload-button').off('click').on('click', () => void loadPage());
        $('#save-button').off('click').on('click', () => void handleSaveClick());
        $('#add-button').off('click').on('click', () => void addStateId());
        $('#browse-button').off('click').on('click', () => void openPicker());
        $('#new-state-id').off('keypress').on('keypress', (event) => {
            if (event.key === 'Enter') {
                event.preventDefault();
                void addStateId();
            }
        });
        $('#picker-search').off('input').on('input', () => renderPickerList());
        $('#picker-select-visible').off('click').on('click', () => selectVisiblePickerItems(true));
        $('#picker-clear-visible').off('click').on('click', () => selectVisiblePickerItems(false));
        $('#picker-apply').off('click').on('click', () => void applyPickerSelection());
        $('#object-picker-modal').modal();
        $('#delete-confirm-modal').modal();
        $('#delete-confirm-apply').off('click').on('click', () => applyDeleteConfirmed());
    }

    async function loadPage() {
        removedStateIds = new Set();
        debugLog('loadPage start', { instanceObjectId });

        const instanceObject = await getAnyObject(instanceObjectId);
        if (!instanceObject) {
            throw new Error(`Instanzobjekt nicht gefunden: ${instanceObjectId}`);
        }

        updateSummary(instanceObject, []);

        const stateView = await getObjectView('system', 'state', {
            startkey: '',
            endkey: '\u9999'
        });

        loadedStateObjects = new Map(
            (stateView.rows || [])
                .map((row) => [row?.id || row?.value?._id, row?.value])
                .filter(([id, value]) => Boolean(id && value))
        );

        rows = (stateView.rows || [])
            .map((row) => mapRowToChannel(row))
            .filter(Boolean)
            .sort((a, b) => String(a.stateId).localeCompare(String(b.stateId)));
        initiallyLoadedStateIds = new Set(rows.map((row) => row.stateId));

        debugLog('loadPage finished', {
            configuredRows: rows.length,
            totalStates: (stateView.rows || []).length
        });

        renderRows();
        updateSummary(instanceObject, rows);
    }

    async function openPicker() {
        if (!availableStates.length) {
            await loadAvailableStates();
        }

        pickerSelection = new Set(rows.map((row) => row.stateId));
        $('#picker-search').val('');
        renderPickerList();
        $('#object-picker-modal').modal('open');
    }

    function renderPickerList() {
        const query = String($('#picker-search').val() || '').trim().toLowerCase();
        const filteredStates = availableStates.filter((stateId) => !query || String(stateId).toLowerCase().includes(query));
        const $list = $('#picker-list');
        $list.empty();

        if (!filteredStates.length) {
            $list.append('<div class="picker-empty">Keine passenden Datenpunkte gefunden.</div>');
            updatePickerSummary(0);
            return;
        }

        filteredStates.forEach((stateId) => {
            const checked = pickerSelection.has(stateId) ? 'checked' : '';
            const $item = $(`
                <div class="picker-item">
                    <label>
                        <input type="checkbox" class="filled-in picker-checkbox" data-state-id="${escapeHtml(stateId)}" ${checked} />
                        <span class="picker-id">${escapeHtml(stateId)}</span>
                    </label>
                </div>
            `);

            $item.find('.picker-checkbox').on('change', function () {
                const currentStateId = String($(this).data('state-id'));
                if ($(this).prop('checked')) {
                    pickerSelection.add(currentStateId);
                } else {
                    pickerSelection.delete(currentStateId);
                }
                updatePickerSummary(filteredStates.length);
            });

            $list.append($item);
        });

        updatePickerSummary(filteredStates.length);
    }

    function updatePickerSummary(visibleCount) {
        $('#picker-summary').text(`${visibleCount} sichtbar, ${pickerSelection.size} markiert`);
    }

    function selectVisiblePickerItems(selected) {
        const query = String($('#picker-search').val() || '').trim().toLowerCase();
        const filteredStates = availableStates.filter((stateId) => !query || String(stateId).toLowerCase().includes(query));

        filteredStates.forEach((stateId) => {
            if (selected) {
                pickerSelection.add(stateId);
            } else {
                pickerSelection.delete(stateId);
            }
        });

        renderPickerList();
    }

    async function applyPickerSelection() {
        const selectedStateIds = [...pickerSelection].sort((a, b) => String(a).localeCompare(String(b)));
        const currentStateIds = new Set(rows.map((row) => row.stateId));

        rows.forEach((row) => {
            if (!pickerSelection.has(row.stateId)) {
                removedStateIds.add(row.stateId);
            }
        });

        rows = rows.filter((row) => pickerSelection.has(row.stateId));

        for (const stateId of selectedStateIds) {
            if (currentStateIds.has(stateId)) {
                continue;
            }

            rows.push({
                stateId,
                key: objectIdToFirebaseKey(stateId),
                sync: true,
                mode: DEFAULT_ROW.mode,
                minChange: DEFAULT_ROW.minChange,
                factor: DEFAULT_ROW.factor,
                transform: DEFAULT_ROW.transform,
                timeUnit: DEFAULT_ROW.timeUnit,
                dailyHour: DEFAULT_ROW.dailyHour,
                dailyMinute: DEFAULT_ROW.dailyMinute,
                round: DEFAULT_ROW.round,
                minSendIntervalMs: DEFAULT_ROW.minSendIntervalMs,
                maxSendIntervalMs: DEFAULT_ROW.maxSendIntervalMs,
                defaultValue: DEFAULT_ROW.defaultValue
            });
        }

        rows.sort((a, b) => a.stateId.localeCompare(b.stateId));
        renderRows();
        updateSummaryDisplay(rows);
        $('#object-picker-modal').modal('close');
        showToast(`${selectedStateIds.length} Datenpunkte ausgewählt. Änderungen noch nicht gespeichert.`);
    }

    function mapRowToChannel(row) {
        const object = row?.value;
        const stateId = row?.id || object?._id;
        const customMap = object?.common?.custom || {};
        const custom = customMap[namespace] || customMap[ADAPTER];
        if (!stateId || !custom?.enabled) {
            return null;
        }

        return {
            stateId,
            key: custom.key || objectIdToFirebaseKey(stateId),
            sync: custom.sync !== false,
            mode: custom.mode || DEFAULT_ROW.mode,
            minChange: custom.minChange ?? DEFAULT_ROW.minChange,
            factor: custom.factor ?? DEFAULT_ROW.factor,
            transform: custom.transform || DEFAULT_ROW.transform,
            timeUnit: normalizeTimeUnit(custom.timeUnit),
            dailyHour: normalizeDailyHour(custom.dailyHour),
            dailyMinute: normalizeDailyMinute(custom.dailyMinute),
            round: custom.round ?? DEFAULT_ROW.round,
            minSendIntervalMs: custom.minSendIntervalMs ?? DEFAULT_ROW.minSendIntervalMs,
            maxSendIntervalMs: custom.maxSendIntervalMs ?? DEFAULT_ROW.maxSendIntervalMs,
            defaultValue: custom.defaultValue ?? ''
        };
    }

    async function loadAvailableStates() {
        const stateView = await getObjectView('system', 'state', {
            startkey: '',
            endkey: '\u9999'
        });

        availableStates = (stateView.rows || [])
            .map((row) => row?.id || row?.value?._id)
            .filter(Boolean)
            .sort((a, b) => String(a).localeCompare(String(b)));
    }

    function renderRows(emptyMessage) {
        const $body = $('#channel-table-body');
        $body.empty();

        if (!rows.length) {
            $body.append(`<div class="empty-row">${emptyMessage || 'Keine Datenpunkte ausgewählt.'}</div>`);
            return;
        }

        rows.forEach((row, index) => {
            const isDaily = row.mode === 'daily_only';
            const $card = $(`
                <div class="channel-card ${isDaily ? 'mode-daily' : 'mode-interval'}" data-index="${index}">
                    <div class="channel-summary">
                        <div class="summary-sync">
                            ${renderSummaryControls(row.sync)}
                        </div>
                        <div class="summary-field">
                            <span class="summary-label-inline">State ID</span>
                            <span class="summary-value-inline">${escapeHtml(row.stateId)}</span>
                        </div>
                        <div class="summary-field key-field">
                            <span class="summary-label-inline">Firebase Key</span>
                            <span class="summary-value-inline">${escapeHtml(row.key)}</span>
                        </div>
                        <div class="summary-toggle">&#9662;</div>
                    </div>
                    <div class="channel-details">
                        <div class="details-grid">
                            <div class="detail-field span-2">
                                <label>State ID</label>
                                <input class="state-id" type="text" data-field="stateId" value="${escapeHtml(row.stateId)}" />
                            </div>
                            <div class="detail-field">
                                <label class="label-with-info">
                                    Firebase Key
                                    <span class="info-icon" tabindex="0" aria-label="Info zu Firebase Key">i</span>
                                </label>
                                <input class="channel-key" type="text" data-field="key" value="${escapeHtml(row.key)}" />
                            </div>
                            <div class="detail-field">
                                <label>Mode</label>
                                ${renderSelect('mode', row.mode, [
                                    { value: 'threshold', label: 'Schwellwert' },
                                    { value: 'change', label: 'Jede Änderung (bei Wertwechsel)' },
                                    { value: 'daily_only', label: 'Täglich (Uhrzeit)' }
                                ])}
                            </div>
                            <div class="detail-field">
                                <label>Transform</label>
                                ${renderSelect('transform', row.transform, [
                                    { value: 'none', label: 'Keine Umwandlung' },
                                    { value: 'positive_only', label: 'Nur positiv (negative Werte werden 0)' },
                                    { value: 'boolean', label: 'Boolean (0 oder 1)' }
                                ])}
                            </div>
                            <div class="detail-field">
                                <label>Zeiteinheit</label>
                                ${renderSelect('timeUnit', row.timeUnit, [
                                    { value: 'ms', label: 'Millisekunden' },
                                    { value: 's', label: 'Sekunden' },
                                    { value: 'min', label: 'Minuten' },
                                    { value: 'h', label: 'Stunden' },
                                    { value: 'd', label: 'Tage' }
                                ])}
                            </div>
                            <div class="detail-field">
                                <label>Mindeständerung</label>
                                <input type="number" step="0.1" data-field="minChange" value="${row.minChange}" />
                            </div>
                            <div class="detail-field">
                                <label>Faktor</label>
                                <input type="number" step="0.1" data-field="factor" value="${row.factor}" />
                            </div>
                            <div class="detail-field">
                                <label>Round</label>
                                <input type="number" step="1" data-field="round" value="${row.round}" />
                            </div>
                            <div class="detail-field mode-not-daily">
                                <label>Min Sendeintervall</label>
                                <div class="input-with-unit">
                                    <input type="number" step="any" data-field="minSendIntervalMs" value="${formatMsForUnit(row.minSendIntervalMs, row.timeUnit)}" />
                                    <span class="unit-suffix">${formatTimeUnitDisplay(row.timeUnit)}</span>
                                </div>
                                <div class="field-help">Kürzester Abstand zwischen zwei Schreibvorgängen.</div>
                            </div>
                            <div class="detail-field mode-not-daily">
                                <label>Max Sendeintervall</label>
                                <div class="input-with-unit">
                                    <input type="number" step="any" data-field="maxSendIntervalMs" value="${formatMsForUnit(row.maxSendIntervalMs, row.timeUnit)}" />
                                    <span class="unit-suffix">${formatTimeUnitDisplay(row.timeUnit)}</span>
                                </div>
                                <div class="field-help">Spätestens nach diesem Abstand wird erneut geschrieben.</div>
                            </div>
                            <div class="detail-field mode-daily-only">
                                <label>Uhrzeit</label>
                                <input type="time" data-field="dailyTime" value="${formatDailyTime(row.dailyHour, row.dailyMinute)}" />
                                <div class="field-help">Wann täglich geschrieben wird. Aktuelle Systemzeit: <span class="system-time-now">${getSystemTimeText()}</span></div>
                            </div>
                            <div class="detail-field">
                                <label>Default</label>
                                <input type="number" step="0.1" data-field="defaultValue" value="${formatInputValue(row.defaultValue)}" />
                                <div class="field-help">Wert für den Startfall oder wenn kein gültiger Messwert vorliegt.</div>
                            </div>
                        </div>
                    </div>
                </div>
            `);

            $card.find('.channel-summary').on('click', function (event) {
                if ($(event.target).closest('.summary-sync').length) {
                    return;
                }
                $card.toggleClass('open');
            });
            $card.find('[data-field]').not('[data-field="timeUnit"]').not('[data-field="mode"]').on('change keyup', () => syncRowFromDom($card));
            $card.find('[data-field="timeUnit"]').on('change', () => handleTimeUnitChange($card));
            $card.find('[data-field="mode"]').on('change', () => handleModeChange($card));
            $card.find('.info-icon')
                .on('mouseenter focus', (event) => {
                    showInfoTooltip(
                        $(event.currentTarget),
                        'Der Firebase Key ist der Zielpfad in Firebase. Beispiel: custom/0_userdata/0/temperatur. "/" erzeugt Unterordner.'
                    );
                })
                .on('mouseleave blur', () => hideInfoTooltip());
            $card.find('.summary-delete').on('click', (event) => {
                event.stopPropagation();
                confirmRemoveRow(index);
            });
            $card.find('[data-field="sync"]').on('click', (event) => event.stopPropagation());
            updateModeUi($card, row.mode);
            $body.append($card);
        });

        updateSystemTimeHints();
    }

    function renderSummaryControls(value) {
        return `
            <label class="summary-sync-toggle">
                <input type="checkbox" data-field="sync" ${value ? 'checked' : ''} />
                <span></span>
            </label>
            <button type="button" class="summary-delete" title="Datenpunkt löschen" aria-label="Datenpunkt löschen">
                <span aria-hidden="true">&#128465;</span>
            </button>
        `;
    }

    function renderSelect(field, value, options) {
        const optionHtml = options
            .map((item) => {
                const optionValue = typeof item === 'string' ? item : item.value;
                const optionLabel = typeof item === 'string' ? item : item.label;
                return `<option value="${optionValue}" ${optionValue === value ? 'selected' : ''}>${optionLabel}</option>`;
            })
            .join('');
        return `<select class="browser-default" data-field="${field}">${optionHtml}</select>`;
    }

    function syncRowFromDom($tr) {
        const index = Number($tr.attr('data-index'));
        const row = rows[index];
        if (!row) {
            return;
        }

        row.stateId = String($tr.find('[data-field="stateId"]').val() || '').trim();
        row.key = String($tr.find('[data-field="key"]').val() || '').trim();
        row.sync = Boolean($tr.find('[data-field="sync"]').prop('checked'));
        row.mode = String($tr.find('[data-field="mode"]').val() || DEFAULT_ROW.mode);
        row.minChange = toNumber($tr.find('[data-field="minChange"]').val(), DEFAULT_ROW.minChange);
        row.factor = toNumber($tr.find('[data-field="factor"]').val(), DEFAULT_ROW.factor);
        row.transform = String($tr.find('[data-field="transform"]').val() || DEFAULT_ROW.transform);
        row.timeUnit = normalizeTimeUnit($tr.find('[data-field="timeUnit"]').val());
        const dailyTime = parseDailyTime(String($tr.find('[data-field="dailyTime"]').val() || ''), row.dailyHour, row.dailyMinute);
        row.dailyHour = dailyTime.hour;
        row.dailyMinute = dailyTime.minute;
        row.round = toNumber($tr.find('[data-field="round"]').val(), DEFAULT_ROW.round);
        row.minSendIntervalMs = toMsByUnit($tr.find('[data-field="minSendIntervalMs"]').val(), row.timeUnit, DEFAULT_ROW.minSendIntervalMs);
        row.maxSendIntervalMs = toMsByUnit($tr.find('[data-field="maxSendIntervalMs"]').val(), row.timeUnit, DEFAULT_ROW.maxSendIntervalMs);
        row.defaultValue = normalizeDefaultValue($tr.find('[data-field="defaultValue"]').val());

        const $card = $tr.closest('.channel-card');
        $card.find('.summary-field .summary-value-inline').eq(0).text(row.stateId);
        $card.find('.summary-field .summary-value-inline').eq(1).text(row.key);
        updateSummaryDisplay(rows);
    }

    function handleTimeUnitChange($tr) {
        const index = Number($tr.attr('data-index'));
        const row = rows[index];
        if (!row) {
            return;
        }

        const oldUnit = normalizeTimeUnit(row.timeUnit);
        const newUnit = normalizeTimeUnit($tr.find('[data-field="timeUnit"]').val());
        const currentMinMs = toMsByUnit($tr.find('[data-field="minSendIntervalMs"]').val(), oldUnit, row.minSendIntervalMs);
        const currentMaxMs = toMsByUnit($tr.find('[data-field="maxSendIntervalMs"]').val(), oldUnit, row.maxSendIntervalMs);

        $tr.find('[data-field="minSendIntervalMs"]').val(formatMsForUnit(currentMinMs, newUnit));
        $tr.find('[data-field="maxSendIntervalMs"]').val(formatMsForUnit(currentMaxMs, newUnit));
        $tr.find('.unit-suffix').text(formatTimeUnitDisplay(newUnit));

        row.timeUnit = newUnit;
        row.minSendIntervalMs = currentMinMs;
        row.maxSendIntervalMs = currentMaxMs;
        syncRowFromDom($tr);
    }

    function handleModeChange($tr) {
        const mode = String($tr.find('[data-field="mode"]').val() || DEFAULT_ROW.mode);
        updateModeUi($tr, mode);
        syncRowFromDom($tr);
    }

    function updateModeUi($tr, mode) {
        const isDaily = mode === 'daily_only';
        const isChange = mode === 'change';
        $tr.toggleClass('mode-daily', isDaily);
        $tr.toggleClass('mode-interval', !isDaily);
        $tr.find('[data-field="minChange"]').prop('disabled', isChange);
    }

    function ensureInfoTooltip() {
        if (infoTooltipEl) {
            return infoTooltipEl;
        }
        infoTooltipEl = $('<div class="firebase-info-tooltip"></div>');
        $('body').append(infoTooltipEl);
        return infoTooltipEl;
    }

    function showInfoTooltip($target, text) {
        const $tooltip = ensureInfoTooltip();
        $tooltip.text(text).addClass('visible');

        const offset = $target.offset();
        if (!offset) {
            return;
        }

        const top = offset.top + $target.outerHeight() + 8;
        const left = Math.max(10, offset.left - 8);
        $tooltip.css({ top: `${top}px`, left: `${left}px` });
    }

    function hideInfoTooltip() {
        if (infoTooltipEl) {
            infoTooltipEl.removeClass('visible');
        }
    }

    function confirmRemoveRow(index) {
        if (skipDeleteConfirmForSession) {
            removeRow(index);
            return;
        }

        pendingDeleteIndex = index;
        const row = rows[index];
        $('#delete-confirm-text').text(`Soll der Datenpunkt "${row?.stateId || ''}" wirklich entfernt werden?`);
        $('#delete-confirm-session').prop('checked', false);
        $('#delete-confirm-modal').modal('open');
    }

    function applyDeleteConfirmed() {
        skipDeleteConfirmForSession = $('#delete-confirm-session').prop('checked');
        $('#delete-confirm-modal').modal('close');
        if (pendingDeleteIndex !== null) {
            removeRow(pendingDeleteIndex);
            pendingDeleteIndex = null;
        }
    }

    function removeRow(index) {
        const row = rows[index];
        if (row?.stateId) {
            removedStateIds.add(row.stateId);
        }

        rows = rows.filter((_, currentIndex) => currentIndex !== index);
        renderRows();
        updateSummaryDisplay(rows);
        showToast('Datenpunkt aus der Liste entfernt. Änderungen noch nicht gespeichert.');
    }

    async function addStateId() {
        const stateId = String($('#new-state-id').val() || '').trim();
        if (!stateId) {
            showToast('Bitte eine State-ID eingeben.');
            return;
        }

        if (rows.some((row) => row.stateId === stateId)) {
            showToast('State-ID ist bereits in der Liste.');
            return;
        }

        const object = await getAnyObject(stateId);
        if (!object || object.type !== 'state') {
            showToast('State-ID wurde nicht gefunden.');
            return;
        }

        loadedStateObjects.set(stateId, object);

        rows.push({
            stateId,
            key: objectIdToFirebaseKey(stateId),
            sync: true,
            mode: DEFAULT_ROW.mode,
            minChange: DEFAULT_ROW.minChange,
            factor: DEFAULT_ROW.factor,
            transform: DEFAULT_ROW.transform,
            timeUnit: DEFAULT_ROW.timeUnit,
            dailyHour: DEFAULT_ROW.dailyHour,
            dailyMinute: DEFAULT_ROW.dailyMinute,
            round: DEFAULT_ROW.round,
            minSendIntervalMs: DEFAULT_ROW.minSendIntervalMs,
            maxSendIntervalMs: DEFAULT_ROW.maxSendIntervalMs,
            defaultValue: DEFAULT_ROW.defaultValue
        });

        rows.sort((a, b) => a.stateId.localeCompare(b.stateId));
        $('#new-state-id').val('');
        renderRows();
        updateSummaryDisplay(rows);
        showToast('State-ID hinzugefügt. Änderungen noch nicht gespeichert.');
    }

    async function handleSaveClick() {
        if (isSaving) {
            debugLog('handleSaveClick ignored because save is already running');
            return;
        }

        try {
            setSavingState(true);
            showToast('Speichern läuft...');
            debugLog('save button clicked', {
                rows: rows.length,
                removedStateIds: [...removedStateIds]
            });
            await saveRows();
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            setStatus(`Fehler beim Speichern: ${message}`);
            showToast(`Speichern fehlgeschlagen: ${message}`);
            debugError('saveRows failed', error);
        } finally {
            setSavingState(false);
        }
    }

    async function saveRows(silent) {
        syncAllRowsFromDom();
        const currentStateIds = new Set(rows.map((row) => row.stateId));
        debugLog('saveRows start', {
            rows: rows.map((row) => ({
                stateId: row.stateId,
                sync: row.sync,
                key: row.key
            })),
            removedStateIds: [...removedStateIds]
        });

        const instanceObject = await getAnyObject(instanceObjectId);
        if (!instanceObject) {
            throw new Error(`Instanzobjekt nicht gefunden: ${instanceObjectId}`);
        }

        const configuredStateIds = Array.isArray(instanceObject.native?.channels)
            ? instanceObject.native.channels
                .map((channel) => String(channel?.stateId || '').trim())
                .filter(Boolean)
            : [];
        const stateIdsToRemove = new Set(removedStateIds);

        configuredStateIds.forEach((stateId) => {
            if (!currentStateIds.has(stateId)) {
                stateIdsToRemove.add(stateId);
            }
        });
        initiallyLoadedStateIds.forEach((stateId) => {
            if (!currentStateIds.has(stateId)) {
                stateIdsToRemove.add(stateId);
            }
        });

        const rowMap = new Map(rows.map((row) => [row.stateId, normalizeRow(row)]));
        debugLog('saveRows removal reconciliation', {
            configuredStateIds,
            initiallyLoadedStateIds: [...initiallyLoadedStateIds],
            currentStateIds: [...currentStateIds],
            stateIdsToRemove: [...stateIdsToRemove]
        });

        debugLog('SAVE_ORDER_V3 instance-first writing instance object', {
            instanceObjectId,
            channelCount: rows.length
        });
        instanceObject.native.channels = rows.map((row) => ({
            key: row.key,
            stateId: row.stateId,
            enabled: true,
            sync: row.sync,
            mode: row.mode,
            minChange: row.minChange,
            factor: row.factor,
            transform: row.transform,
            timeUnit: row.timeUnit,
            dailyHour: row.dailyHour,
            dailyMinute: row.dailyMinute,
            round: row.round,
            minSendIntervalMs: row.minSendIntervalMs,
            maxSendIntervalMs: row.maxSendIntervalMs,
            defaultValue: normalizeDefaultValue(row.defaultValue)
        }));
        await setAnyObject(instanceObjectId, instanceObject);
        debugLog('SAVE_ORDER_V3 instance-first wrote instance object successfully', {
            instanceObjectId,
            channelCount: instanceObject.native.channels.length
        });

        debugLog('saveRows defers state custom reconciliation to backend polling', {
            namespace,
            stateIdsToRemove: [...stateIdsToRemove],
            rowStateIds: rows.map((row) => row.stateId)
        });

        removedStateIds = new Set();
        initiallyLoadedStateIds = new Set(rows.map((row) => row.stateId));
        updateSummary(instanceObject, rows);
        renderRows();
        if (!silent) {
            showToast('Channels gespeichert. Backend synchronisiert die Objektkonfiguration in Kürze.');
        }
        setStatus(`Instanz ${namespace} gespeichert`);
        debugLog('saveRows finished successfully');
    }

    function syncAllRowsFromDom() {
        $('#channel-table-body .channel-card').each(function () {
            syncRowFromDom($(this));
        });
    }

    function normalizeRow(row) {
        return {
            enabled: true,
            sync: row.sync,
            key: row.key,
            mode: row.mode,
            minChange: row.minChange,
            factor: row.factor,
            transform: row.transform,
            timeUnit: row.timeUnit,
            dailyHour: row.dailyHour,
            dailyMinute: row.dailyMinute,
            round: row.round,
            minSendIntervalMs: row.minSendIntervalMs,
            maxSendIntervalMs: row.maxSendIntervalMs,
            defaultValue: normalizeDefaultValue(row.defaultValue)
        };
    }

    function normalizeTimeUnit(value) {
        const unit = String(value || '').trim().toLowerCase();
        if (unit === 'ms' || unit === 's' || unit === 'min' || unit === 'h' || unit === 'd') {
            return unit;
        }
        return DEFAULT_ROW.timeUnit;
    }

    function normalizeDailyHour(value) {
        const parsed = Number(value);
        if (!Number.isFinite(parsed)) {
            return DEFAULT_ROW.dailyHour;
        }
        return Math.max(0, Math.min(23, Math.round(parsed)));
    }

    function normalizeDailyMinute(value) {
        const parsed = Number(value);
        if (!Number.isFinite(parsed)) {
            return DEFAULT_ROW.dailyMinute;
        }
        return Math.max(0, Math.min(59, Math.round(parsed)));
    }

    function parseDailyTime(value, fallbackHour, fallbackMinute) {
        const match = String(value || '').match(/^(\d{1,2}):(\d{2})$/);
        if (!match) {
            return {
                hour: normalizeDailyHour(fallbackHour),
                minute: normalizeDailyMinute(fallbackMinute)
            };
        }
        return {
            hour: normalizeDailyHour(Number(match[1])),
            minute: normalizeDailyMinute(Number(match[2]))
        };
    }

    function formatDailyTime(hour, minute) {
        const normalizedHour = String(normalizeDailyHour(hour)).padStart(2, '0');
        const normalizedMinute = String(normalizeDailyMinute(minute)).padStart(2, '0');
        return `${normalizedHour}:${normalizedMinute}`;
    }

    function unitToMs(unit) {
        switch (unit) {
            case 'd':
                return 86400000;
            case 'h':
                return 3600000;
            case 'min':
                return 60000;
            case 's':
                return 1000;
            case 'ms':
            default:
                return 1;
        }
    }

    function toMsByUnit(value, unit, fallbackMs) {
        const parsed = Number(value);
        if (!Number.isFinite(parsed)) {
            return fallbackMs;
        }
        return Math.max(0, Math.round(parsed * unitToMs(unit)));
    }

    function formatMsForUnit(msValue, unit) {
        const numericMs = Number(msValue);
        if (!Number.isFinite(numericMs)) {
            return 0;
        }
        const factor = unitToMs(normalizeTimeUnit(unit));
        const converted = numericMs / factor;
        const rounded = Math.round(converted * 1000000000) / 1000000000;
        return Number.isFinite(rounded) ? rounded : converted;
    }

    function formatTimeUnitDisplay(unit) {
        const normalized = normalizeTimeUnit(unit);
        if (normalized === 'ms') {
            return 'Millisekunden';
        }
        if (normalized === 's') {
            return 'Sekunden';
        }
        if (normalized === 'min') {
            return 'Minuten';
        }
        if (normalized === 'h') {
            return 'Stunden';
        }
        if (normalized === 'd') {
            return 'Tage';
        }
        return normalized;
    }

    function getSystemTimeText() {
        const now = new Date();
        const date = now.toLocaleDateString('de-AT');
        const time = now.toLocaleTimeString('de-AT', { hour12: false });
        return `${date} ${time}`;
    }

    function updateSystemTimeHints() {
        $('.system-time-now').text(getSystemTimeText());
    }

    function startSystemTimeTicker() {
        if (systemTimeIntervalId) {
            clearInterval(systemTimeIntervalId);
        }
        updateSystemTimeHints();
        systemTimeIntervalId = setInterval(() => updateSystemTimeHints(), 1000);
    }

    function updateSummary(instanceObject, currentRows) {
        $('#summary-database-url').text(instanceObject?.native?.databaseUrl || '-');
        $('#summary-dry-run').text(instanceObject?.native?.dryRun ? 'Ja' : 'Nein');
        updateSummaryDisplay(currentRows);
    }

    function updateSummaryDisplay(currentRows) {
        $('#summary-count').text(currentRows.length);
        $('#summary-sync-count').text(currentRows.filter((row) => row.sync).length);
    }

    function setStatus(text) {
        $('#instance-label').text(text);
    }

    function setSavingState(saving) {
        isSaving = saving;
        $('#save-button').toggleClass('disabled', saving);
        $('#save-button').attr('aria-disabled', saving ? 'true' : 'false');
        $('#save-button').text(saving ? 'Speichert...' : 'Speichern');
    }

    function detectInstance() {
        const params = new URLSearchParams(window.location.search);
        const directInstance = params.get('instance');
        if (directInstance) {
            return directInstance;
        }

        const objectId = params.get('id');
        if (objectId) {
            const match = objectId.match(/\.([0-9]+)$/);
            if (match) {
                return match[1];
            }
        }

        const hrefMatch = window.location.href.match(/firebase-history-sync(?:\.([0-9]+))?/);
        if (hrefMatch?.[1]) {
            return hrefMatch[1];
        }

        return '0';
    }

    function objectIdToFirebaseKey(objectId) {
        const normalizedPath = String(objectId)
            .split('.')
            .map((part) => part.trim().replace(/[.#$\[\]/]+/g, '_').replace(/^_+|_+$/g, ''))
            .filter(Boolean)
            .join('/');
        return normalizedPath ? `custom/${normalizedPath}` : 'custom/value';
    }

    function normalizeDefaultValue(value) {
        if (value === '' || value === null || value === undefined) {
            return null;
        }

        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : null;
    }

    function toNumber(value, fallback) {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : fallback;
    }

    function escapeHtml(value) {
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function formatInputValue(value) {
        return value === null || value === undefined ? '' : String(value);
    }

    function cloneObjectForWrite(object) {
        if (!object) {
            return null;
        }
        return JSON.parse(JSON.stringify(object));
    }

    function showToast(message) {
        if (window.M && M.toast) {
            M.toast({ html: message, displayLength: 4000 });
        } else {
            debugError(message);
        }
    }

    function debugLog(message, data) {
        if (data !== undefined) {
            console.info(DEBUG_PREFIX, message, data);
            return;
        }
        console.info(DEBUG_PREFIX, message);
    }

    function debugWarn(message, data) {
        if (data !== undefined) {
            console.warn(DEBUG_PREFIX, message, data);
            return;
        }
        console.warn(DEBUG_PREFIX, message);
    }

    function debugError(message, error) {
        if (error !== undefined) {
            console.error(DEBUG_PREFIX, message, error);
            return;
        }
        console.error(DEBUG_PREFIX, message);
    }

    function getAnyObject(id) {
        const command = 'getObject';
        return new Promise((resolve, reject) => {
            const startedAt = Date.now();
            const timeout = setTimeout(() => {
                reject(new Error(`${command} timeout after ${SOCKET_TIMEOUT_MS}ms for ${id}`));
            }, SOCKET_TIMEOUT_MS);

            socket.emit(command, id, (err, obj) => {
                clearTimeout(timeout);
                if (err) {
                    reject(new Error(String(err)));
                    return;
                }

                debugLog('socket get completed', {
                    command,
                    id,
                    durationMs: Date.now() - startedAt
                });
                resolve(obj);
            });
        });
    }

    function setAnyObject(id, obj) {
        const command = isSystemObject(id) ? 'setObject' : 'setForeignObject';
        return new Promise((resolve, reject) => {
            const startedAt = Date.now();
            const timeout = setTimeout(() => {
                reject(new Error(`${command} timeout after ${SOCKET_TIMEOUT_MS}ms for ${id}`));
            }, SOCKET_TIMEOUT_MS);

            socket.emit(command, id, obj, (err) => {
                clearTimeout(timeout);
                if (err) {
                    reject(new Error(String(err)));
                    return;
                }

                debugLog('socket set completed', {
                    command,
                    id,
                    durationMs: Date.now() - startedAt
                });
                resolve();
            });
        });
    }

    function isSystemObject(id) {
        return String(id).startsWith('system.');
    }

    function getObjectView(design, search, params) {
        return new Promise((resolve, reject) => {
            const startedAt = Date.now();
            const timeout = setTimeout(() => {
                reject(new Error(`getObjectView timeout after ${SOCKET_TIMEOUT_MS}ms for ${design}/${search}`));
            }, SOCKET_TIMEOUT_MS);

            socket.emit('getObjectView', design, search, params, (err, result) => {
                clearTimeout(timeout);
                if (err) {
                    reject(new Error(String(err)));
                    return;
                }

                debugLog('socket view completed', {
                    design,
                    search,
                    durationMs: Date.now() - startedAt
                });
                resolve(result);
            });
        });
    }

    function sendToAdapter(command, message) {
        return new Promise((resolve, reject) => {
            const startedAt = Date.now();
            const timeout = setTimeout(() => {
                reject(new Error(`sendTo timeout after ${SOCKET_TIMEOUT_MS}ms for ${namespace}/${command}`));
            }, SOCKET_TIMEOUT_MS);

            socket.emit('sendTo', namespace, command, message, (response) => {
                clearTimeout(timeout);
                debugLog('socket sendTo completed', {
                    namespace,
                    command,
                    durationMs: Date.now() - startedAt,
                    response
                });

                if (response && response.error) {
                    reject(new Error(String(response.error)));
                    return;
                }

                resolve(response);
            });
        });
    }
})();
