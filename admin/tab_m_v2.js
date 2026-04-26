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
    let availableStateValues = new Map();
    let loadingAvailableStateValues = new Set();
    let loadedStateObjects = new Map();
    let loadedReadableRtdbEntries = [];
    let selectedReadableRtdbPaths = new Set();
    let savedReadSubscriptionsByPath = new Map();
    let editingReadableRtdbPath = '';
    let knownRoles = getDefaultKnownRoles();
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
        renderRoleDropdown();
        startSystemTimeTicker();
        setStatus(`Instanz ${namespace}`);
        await loadPage();
    }

    function bindUi() {
        $('#reload-button').off('click').on('click', () => void loadPage());
        $('#save-button').off('click').on('click', () => void handleSaveClick());
        $('#load-rtdb-points-button').off('click').on('click', () => void handleLoadRtdbPointsClick());
        $('#open-create-rtdb-modal-button').off('click').on('click', () => openCreateRtdbModal());
        $('#create-rtdb-point-button').off('click').on('click', () => void handleCreateRtdbDatapointClick());
        $('#delete-selected-rtdb-button').off('click').on('click', () => void handleDeleteSelectedRtdbDatapointsClick());
        $('#subscribe-selected-rtdb-button').off('click').on('click', () => void handleSubscribeSelectedRtdbClick());
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
        $('#create-rtdb-modal').modal();
        $('#delete-confirm-apply').off('click').on('click', () => applyDeleteConfirmed());
    }

    function openCreateRtdbModal() {
        editingReadableRtdbPath = '';
        $('#create-rtdb-modal-title').text('Datenpunkt erstellen');
        $('#create-rtdb-point-button').text('In RTDB erstellen');
        $('#rtdb-create-key').val('');
        $('#rtdb-create-state').val('');
        $('#rtdb-create-type').val('string');
        $('#rtdb-create-role').val('indicator');
        $('#rtdb-create-description').val('');
        $('#create-rtdb-modal').modal('open');
    }

    async function loadPage() {
        removedStateIds = new Set();
        debugLog('loadPage start', { instanceObjectId });

        const instanceObject = await getAnyObject(instanceObjectId);
        if (!instanceObject) {
            throw new Error(`Instanzobjekt nicht gefunden: ${instanceObjectId}`);
        }
        savedReadSubscriptionsByPath = readSubscriptionsToMap(instanceObject?.native?.readSubscriptions);
        selectedReadableRtdbPaths = new Set([...savedReadSubscriptionsByPath.keys()]);
        loadedReadableRtdbEntries = [];

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
        await loadCurrentStateValues(rows);

        debugLog('loadPage finished', {
            configuredRows: rows.length,
            totalStates: (stateView.rows || []).length
        });

        renderRows();
        updateSummary(instanceObject, rows);
        renderRtdbPathList([]);
        $('#rtdb-read-summary').text(`Noch nicht geladen. Gespeichert: ${savedReadSubscriptionsByPath.size} Abos.`);
        await loadKnownRoles();
    }

    async function openPicker() {
        if (!availableStates.length) {
            await loadAvailableStates();
        }

        pickerSelection = new Set(rows.map((row) => row.stateId));
        $('#picker-search').val('');
        renderPickerList();
        $('#object-picker-modal').modal('open');
        void loadPickerStateValues(availableStates);
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
            const object = loadedStateObjects.get(stateId);
            const valueText = availableStateValues.has(stateId)
                ? formatCurrentValueWithUnit({
                    currentValue: availableStateValues.get(stateId),
                    unit: getStateUnit(object)
                })
                : '...';
            const $item = $(`
                <div class="picker-item">
                    <label>
                        <input type="checkbox" class="filled-in picker-checkbox" data-state-id="${escapeHtml(stateId)}" ${checked} />
                        <span class="picker-text">
                            <span class="picker-id">${escapeHtml(stateId)}</span>
                            <span class="picker-value" data-state-id="${escapeHtml(stateId)}" title="${escapeHtml(valueText)}">${escapeHtml(valueText)}</span>
                        </span>
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
        void loadPickerStateValues(filteredStates);
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

            const object = loadedStateObjects.get(stateId);
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
                defaultValue: DEFAULT_ROW.defaultValue,
                currentValue: null,
                unit: getStateUnit(object)
            });
        }

        rows.sort((a, b) => a.stateId.localeCompare(b.stateId));
        await loadCurrentStateValues(rows.filter((row) => selectedStateIds.includes(row.stateId)));
        renderRows();
        updateSummaryDisplay(rows);
        $('#object-picker-modal').modal('close');
        showToast(`${selectedStateIds.length} Datenpunkte ausgew\u00E4hlt. \u00C4nderungen noch nicht gespeichert.`);
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
            defaultValue: custom.defaultValue ?? '',
            currentValue: null,
            unit: getStateUnit(object)
        };
    }

    async function loadCurrentStateValues(channelRows) {
        await Promise.all(channelRows.map(async (row) => {
            try {
                const state = await getAnyState(row.stateId);
                row.currentValue = state ? state.val : null;
            } catch (error) {
                row.currentValue = null;
                debugWarn(`Could not read current state value for ${row.stateId}`, error);
            }
        }));
    }

    async function loadAvailableStates() {
        const stateView = await getObjectView('system', 'state', {
            startkey: '',
            endkey: '\u9999'
        });

        const stateEntries = (stateView.rows || [])
            .map((row) => ({
                id: row?.id || row?.value?._id,
                object: row?.value
            }))
            .filter((entry) => Boolean(entry.id));

        stateEntries.forEach((entry) => {
            if (entry.object) {
                loadedStateObjects.set(entry.id, entry.object);
            }
        });
        availableStates = stateEntries
            .map((entry) => entry.id)
            .sort((a, b) => String(a).localeCompare(String(b)));
        availableStateValues = new Map();
        loadingAvailableStateValues = new Set();
    }

    async function loadPickerStateValues(stateIds) {
        const pendingStateIds = [...new Set(stateIds)]
            .filter((stateId) => !availableStateValues.has(stateId) && !loadingAvailableStateValues.has(stateId));
        if (!pendingStateIds.length) {
            return;
        }

        const batchSize = 50;
        pendingStateIds.forEach((stateId) => loadingAvailableStateValues.add(stateId));

        for (let index = 0; index < pendingStateIds.length; index += batchSize) {
            const batch = pendingStateIds.slice(index, index + batchSize);
            await Promise.all(batch.map(async (stateId) => {
                try {
                    const state = await getAnyState(stateId);
                    availableStateValues.set(stateId, state ? state.val : null);
                    updatePickerStateValue(stateId);
                } catch (error) {
                    availableStateValues.set(stateId, null);
                    updatePickerStateValue(stateId);
                    debugWarn(`Could not read picker state value for ${stateId}`, error);
                } finally {
                    loadingAvailableStateValues.delete(stateId);
                }
            }));
        }
    }

    function updatePickerStateValue(stateId) {
        const object = loadedStateObjects.get(stateId);
        const valueText = formatCurrentValueWithUnit({
            currentValue: availableStateValues.get(stateId),
            unit: getStateUnit(object)
        });
        const selector = `.picker-value[data-state-id="${escapeSelectorValue(stateId)}"]`;
        $(selector).text(valueText).attr('title', valueText);
    }

    function renderRows(emptyMessage) {
        const $body = $('#channel-table-body');
        $body.empty();

        if (!rows.length) {
            $body.append(`<div class="empty-row">${emptyMessage || 'Keine Datenpunkte ausgew\u00E4hlt.'}</div>`);
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
                        <div class="summary-field current-value-field">
                            <span class="summary-label-inline">Aktueller Wert</span>
                            <span class="summary-value-inline current-value-inline">${escapeHtml(formatCurrentValueWithUnit(row))}</span>
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
                                    { value: 'change', label: 'Jede \u00C4nderung (bei Wertwechsel)' },
                                    { value: 'daily_only', label: 'T\u00E4glich (Uhrzeit)' }
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
                                <label>Mindest\u00E4nderung</label>
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
                                <div class="field-help">K\u00FCrzester Abstand zwischen zwei Schreibvorg\u00E4ngen.</div>
                            </div>
                            <div class="detail-field mode-not-daily">
                                <label>Max Sendeintervall</label>
                                <div class="input-with-unit">
                                    <input type="number" step="any" data-field="maxSendIntervalMs" value="${formatMsForUnit(row.maxSendIntervalMs, row.timeUnit)}" />
                                    <span class="unit-suffix">${formatTimeUnitDisplay(row.timeUnit)}</span>
                                </div>
                                <div class="field-help">Sp\u00E4testens nach diesem Abstand wird erneut geschrieben.</div>
                            </div>
                            <div class="detail-field mode-daily-only">
                                <label>Uhrzeit</label>
                                <input type="time" data-field="dailyTime" value="${formatDailyTime(row.dailyHour, row.dailyMinute)}" />
                                <div class="field-help">Wann t\u00E4glich geschrieben wird. Aktuelle Systemzeit: <span class="system-time-now">${getSystemTimeText()}</span></div>
                            </div>
                            <div class="detail-field">
                                <label>Default</label>
                                <input type="number" step="0.1" data-field="defaultValue" value="${formatInputValue(row.defaultValue)}" />
                                <div class="field-help">Wert f\u00FCr den Startfall oder wenn kein g\u00FCltiger Messwert vorliegt.</div>
                            </div>
                            <div class="detail-field">
                                <label>Aktueller Wert</label>
                                <div class="readonly-value">${escapeHtml(formatCurrentValue(row.currentValue))}</div>
                                <div class="field-help">Einheit: ${escapeHtml(formatUnit(row.unit))}</div>
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
            <button type="button" class="summary-delete" title="Datenpunkt l\u00F6schen" aria-label="Datenpunkt l\u00F6schen">
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
        $card.find('.current-value-inline').text(formatCurrentValueWithUnit(row));
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
        showToast('Datenpunkt aus der Liste entfernt. \u00C4nderungen noch nicht gespeichert.');
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
            defaultValue: DEFAULT_ROW.defaultValue,
            currentValue: null,
            unit: getStateUnit(object)
        });

        rows.sort((a, b) => a.stateId.localeCompare(b.stateId));
        await loadCurrentStateValues(rows.filter((row) => row.stateId === stateId));
        $('#new-state-id').val('');
        renderRows();
        updateSummaryDisplay(rows);
        showToast('State-ID hinzugef\u00FCgt. \u00C4nderungen noch nicht gespeichert.');
    }    async function handleLoadRtdbPointsClick() {
        const relativePath = String($('#rtdb-read-path').val() || '').trim() || 'data';
        const $button = $('#load-rtdb-points-button');
        $button.addClass('disabled').attr('aria-disabled', 'true').text('Lädt...');
        try {
            const response = await sendToAdapter('listReadableRtdbPaths', {
                path: relativePath,
                maxEntries: 800,
                maxDepth: 10
            });

            const entries = Array.isArray(response && response.entries) ? response.entries : [];
            const fullPath = response && response.path ? String(response.path) : relativePath;
            const truncated = Boolean(response && response.truncated);
            loadedReadableRtdbEntries = entries
                .map((entry) => ({
                    path: entry && entry.path ? String(entry.path) : '',
                    type: entry && entry.type ? String(entry.type) : 'unknown',
                    preview: entry && entry.preview ? String(entry.preview) : '',
                    role: entry && entry.role ? String(entry.role) : '',
                    description: entry && entry.description ? String(entry.description) : '',
                    name: entry && entry.name ? String(entry.name) : ''
                }))
                .filter((entry) => Boolean(entry.path));
            const summary = truncated
                ? `${loadedReadableRtdbEntries.length} Pfade unter ${fullPath} geladen (gekürzt).`
                : `${loadedReadableRtdbEntries.length} Pfade unter ${fullPath} geladen.`;

            renderRtdbPathList(loadedReadableRtdbEntries);
            $('#rtdb-read-summary').text(summary);
            showToast(summary);
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            $('#rtdb-read-summary').text(`Fehler beim Laden: ${message}`);
            loadedReadableRtdbEntries = [];
            renderRtdbPathList([]);
            showToast(`RTDB-Liste fehlgeschlagen: ${message}`);
            debugError('load readable rtdb paths failed', error);
        } finally {
            $button.removeClass('disabled').attr('aria-disabled', 'false').text('RTDB Liste laden');
        }
    }

    async function handleCreateRtdbDatapointClick() {
        const basePath = normalizeRtdbPath($('#rtdb-read-path').val(), 'data');
        const key = normalizeRtdbPath($('#rtdb-create-key').val(), '');
        if (!editingReadableRtdbPath && !key) {
            showToast('Bitte einen Datenpunktnamen eingeben.');
            return;
        }

        const type = String($('#rtdb-create-type').val() || 'string').trim().toLowerCase();
        const stateRaw = String($('#rtdb-create-state').val() || '').trim();
        const stateValue = parseCreateStateValue(type, stateRaw);
        if (stateValue === undefined) {
            showToast(`Ungültiger state-Wert für Typ "${type}".`);
            return;
        }

        const role = String($('#rtdb-create-role').val() || '').trim();
        const description = String($('#rtdb-create-description').val() || '').trim();
        const path = editingReadableRtdbPath
            ? normalizeRtdbPath(editingReadableRtdbPath, '')
            : normalizeRtdbPath(`${basePath}/${key}`, key);
        const payload = {
            state: stateValue,
            type,
            ...(role ? { role } : {}),
            ...(description ? { description } : {})
        };

        const $button = $('#create-rtdb-point-button');
        $button.addClass('disabled').attr('aria-disabled', 'true');
        try {
            await sendToAdapter('createReadableRtdbDatapoint', { path, data: payload });
            selectedReadableRtdbPaths.add(path);
            showToast(editingReadableRtdbPath ? `RTDB-Datenpunkt aktualisiert: ${path}` : `RTDB-Datenpunkt erstellt: ${path}`);
            editingReadableRtdbPath = '';
            $('#rtdb-create-key').val('');
            $('#rtdb-create-state').val('');
            $('#rtdb-create-type').val('string');
            $('#rtdb-create-role').val('');
            $('#rtdb-create-description').val('');
            $('#create-rtdb-modal-title').text('Datenpunkt erstellen');
            $('#create-rtdb-point-button').text('In RTDB erstellen');
            $('#create-rtdb-modal').modal('close');
            void handleLoadRtdbPointsClick();
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            showToast(`Erstellen fehlgeschlagen: ${message}`);
            debugError('create rtdb datapoint failed', error);
        } finally {
            $button.removeClass('disabled').attr('aria-disabled', 'false');
        }
    }

    async function loadKnownRoles() {
        renderRoleDropdown();
        try {
            const response = await sendToAdapter('listKnownRoles', {});
            const roles = Array.isArray(response && response.roles) ? response.roles : [];
            const merged = roles
                .map((role) => String(role || '').trim())
                .filter(Boolean);
            if (merged.length) {
                knownRoles = [...new Set([...knownRoles, ...merged])].sort((a, b) => a.localeCompare(b));
            }
            renderRoleDropdown();
        } catch (error) {
            debugWarn('could not load known roles', error);
            renderRoleDropdown();
        }
    }

    function renderRoleDropdown() {
        const $select = $('#rtdb-create-role');
        if (!$select.length) {
            return;
        }

        const previousValue = String($select.val() || '').trim();
        const options = []
            .concat(knownRoles)
            .map((role) => `<option value="${escapeHtml(role)}">${escapeHtml(role)}</option>`)
            .join('');
        $select.html(options);

        const hasIndicator = knownRoles.includes('indicator');
        const nextValue = previousValue || (hasIndicator ? 'indicator' : '');
        $select.val(nextValue);
    }

    function getDefaultKnownRoles() {
        return [
            'button',
            'button.close.blind',
            'button.close.tilt',
            'button.fastforward',
            'button.fastreverse',
            'button.forward',
            'button.long',
            'button.mode.',
            'button.mode.auto',
            'button.mode.manual',
            'button.mode.silent',
            'button.next',
            'button.open.blind',
            'button.open.door',
            'button.open.tilt',
            'button.open.window',
            'button.pause',
            'button.play',
            'button.press',
            'button.prev',
            'button.resume',
            'button.reverse',
            'button.start',
            'button.stop',
            'button.stop.tilt',
            'button.volume.down',
            'button.volume.up',
            'indicator',
            'indicator.alarm',
            'indicator.alarm.fire',
            'indicator.alarm.flood',
            'indicator.alarm.health',
            'indicator.alarm.secure',
            'indicator.connected',
            'indicator.direction',
            'indicator.error',
            'indicator.lowbat',
            'indicator.maintenance',
            'indicator.maintenance.alarm',
            'indicator.maintenance.lowbat',
            'indicator.maintenance.unreach',
            'indicator.maintenance.waste',
            'indicator.reachable',
            'indicator.working',
            'sensor',
            'sensor.alarm',
            'sensor.alarm.fire',
            'sensor.alarm.flood',
            'sensor.alarm.power',
            'sensor.alarm.secure',
            'sensor.contact',
            'sensor.door',
            'sensor.light',
            'sensor.lock',
            'sensor.motion',
            'sensor.noise',
            'sensor.rain',
            'sensor.switch',
            'sensor.window',
            'switch',
            'switch.comfort',
            'switch.enable',
            'switch.gate',
            'switch.light',
            'switch.lock',
            'switch.lock.door',
            'switch.lock.window',
            'switch.mode.',
            'switch.mode.auto',
            'switch.mode.boost',
            'switch.mode.color',
            'switch.mode.manual',
            'switch.mode.moonlight',
            'switch.mode.party',
            'switch.mode.silent',
            'switch.pause',
            'switch.power',
            'switch.power.zone',
            'switch.setting'
        ];
    }

    async function handleDeleteSelectedRtdbDatapointsClick() {
        const availablePaths = new Set(
            loadedReadableRtdbEntries
                .map((entry) => (entry && entry.path ? String(entry.path).trim() : ''))
                .filter(Boolean)
        );
        const paths = [...selectedReadableRtdbPaths]
            .filter((path) => availablePaths.has(path))
            .sort((a, b) => String(a).localeCompare(String(b)));
        if (!paths.length) {
            showToast('Keine markierten Datenpunkte zum Löschen.');
            return;
        }

        const confirmed = window.confirm(`${paths.length} markierte Datenpunkte in RTDB wirklich löschen?`);
        if (!confirmed) {
            return;
        }

        const $button = $('#delete-selected-rtdb-button');
        $button.addClass('disabled').attr('aria-disabled', 'true');
        try {
            await sendToAdapter('deleteReadableRtdbDatapoints', { paths });
            paths.forEach((path) => selectedReadableRtdbPaths.delete(path));
            showToast(`${paths.length} Datenpunkte in RTDB gelöscht.`);
            void handleLoadRtdbPointsClick();
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            showToast(`Löschen fehlgeschlagen: ${message}`);
            debugError('delete rtdb datapoints failed', error);
        } finally {
            $button.removeClass('disabled').attr('aria-disabled', 'false');
        }
    }

    async function handleSubscribeSelectedRtdbClick() {
        const availablePaths = new Set(
            loadedReadableRtdbEntries
                .map((entry) => (entry && entry.path ? String(entry.path).trim() : ''))
                .filter(Boolean)
        );
        const markedAvailablePaths = [...selectedReadableRtdbPaths].filter((path) => availablePaths.has(path));
        if (!markedAvailablePaths.length) {
            showToast('Keine markierten Datenpunkte zum Abonnieren.');
            return;
        }

        if (isSaving) {
            return;
        }

        const $button = $('#subscribe-selected-rtdb-button');
        try {
            setSavingState(true);
            $button.addClass('disabled').attr('aria-disabled', 'true');
            await saveRows(true);
            showToast(`${markedAvailablePaths.length} Datenpunkte für ioBroker-Abo gespeichert.`);
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            showToast(`Abo speichern fehlgeschlagen: ${message}`);
            debugError('subscribe selected rtdb failed', error);
        } finally {
            setSavingState(false);
            $button.removeClass('disabled').attr('aria-disabled', 'false');
        }
    }

    async function openEditRtdbDatapointModal(path, entry) {
        const normalizedPath = normalizeRtdbPath(path, '');
        if (!normalizedPath) {
            return;
        }
        const $editButton = $(`.rtdb-edit-button[data-rtdb-path="${escapeSelectorValue(normalizedPath)}"]`);
        $editButton.addClass('disabled').attr('aria-disabled', 'true');
        try {
            const response = await sendToAdapter('readReadableRtdbDatapoint', { path: normalizedPath });
            const raw = response && response.data;
            if (!raw || typeof raw !== 'object' || Array.isArray(raw) || !Object.prototype.hasOwnProperty.call(raw, 'state')) {
                throw new Error('Datenpunkt hat kein gültiges state-Objekt');
            }

            const data = raw;
            const type = normalizeCreateType((data && data.type) || (entry && entry.type) || inferTypeFromValue(data.state));
            const role = String((data && data.role) || (entry && entry.role) || '').trim();
            const description = String((data && data.description) || (entry && entry.description) || '').trim();
            const stateInput = stateValueToInput(type, data.state);
            const key = relativeKeyFromPath(normalizedPath);

            editingReadableRtdbPath = normalizedPath;
            $('#create-rtdb-modal-title').text('Datenpunkt bearbeiten');
            $('#create-rtdb-point-button').text('In RTDB speichern');
            $('#rtdb-create-key').val(key);
            $('#rtdb-create-state').val(stateInput);
            $('#rtdb-create-type').val(type);
            $('#rtdb-create-role').val(role || 'indicator');
            $('#rtdb-create-description').val(description);
            $('#create-rtdb-modal').modal('open');
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            showToast(`Bearbeiten fehlgeschlagen: ${message}`);
            debugError('open edit rtdb datapoint modal failed', error);
        } finally {
            $editButton.removeClass('disabled').attr('aria-disabled', 'false');
        }
    }

    function renderRtdbPathList(entries) {
        const safeEntries = Array.isArray(entries) ? entries : [];
        const $list = $('#rtdb-path-list');
        $list.empty();

        if (!safeEntries.length) {
            $list.append('<div class="picker-empty">Keine Pfade gefunden.</div>');
            updateRtdbFooterActions(safeEntries);
            return;
        }

        safeEntries.forEach((entry) => {
            const path = entry && entry.path ? String(entry.path) : '';
            const type = entry && entry.type ? String(entry.type) : 'unknown';
            const preview = entry && entry.preview ? String(entry.preview) : '';
            const role = entry && entry.role ? String(entry.role) : '';
            const description = entry && entry.description ? String(entry.description) : '';
            const name = entry && entry.name ? String(entry.name) : '';
            const checked = selectedReadableRtdbPaths.has(path) ? 'checked' : '';
            const metaParts = [
                `state: ${preview}`,
                `type: ${type}`,
                role ? `role: ${role}` : '',
                description ? `description: ${description}` : '',
                name ? `name: ${name}` : ''
            ].filter(Boolean);

            const $item = $(`
                <div class="rtdb-path-item">
                    <div class="rtdb-path-head">
                        <label class="rtdb-path-main">
                            <input type="checkbox" class="filled-in rtdb-subscribe-checkbox" data-rtdb-path="${escapeHtml(path)}" ${checked} />
                            <span>${escapeHtml(path)}</span>
                        </label>
                        <a href="#!" class="btn-flat rtdb-edit-button" data-rtdb-path="${escapeHtml(path)}">Bearbeiten</a>
                    </div>
                    <div class="rtdb-path-meta">${escapeHtml(metaParts.join(' | '))}</div>
                </div>
            `);
            $item.find('.rtdb-subscribe-checkbox').on('change', function () {
                const currentPath = String($(this).data('rtdb-path') || '').trim();
                if (!currentPath) {
                    return;
                }
                if ($(this).prop('checked')) {
                    selectedReadableRtdbPaths.add(currentPath);
                } else {
                    selectedReadableRtdbPaths.delete(currentPath);
                }
                $('#rtdb-read-summary').text(
                    `${safeEntries.length} Pfade geladen, ${selectedReadableRtdbPaths.size} für Abo markiert.`
                );
                updateRtdbFooterActions(safeEntries);
            });
            $item.find('.rtdb-edit-button').on('click', (event) => {
                event.preventDefault();
                void openEditRtdbDatapointModal(path, entry);
            });
            $list.append($item);
        });

        $('#rtdb-read-summary').text(
            `${safeEntries.length} Pfade geladen, ${selectedReadableRtdbPaths.size} für Abo markiert.`
        );
        updateRtdbFooterActions(safeEntries);
    }

    function updateRtdbFooterActions(entries) {
        const safeEntries = Array.isArray(entries) ? entries : [];
        const availablePaths = new Set(
            safeEntries
                .map((entry) => (entry && entry.path ? String(entry.path) : ''))
                .filter(Boolean)
        );
        const hasMarkedEntry = [...selectedReadableRtdbPaths].some((path) => availablePaths.has(path));
        const shouldShow = availablePaths.size > 0;
        $('#rtdb-list-footer').toggleClass('is-hidden', !shouldShow);
        $('#delete-selected-rtdb-button').toggleClass('disabled', !hasMarkedEntry).attr('aria-disabled', !hasMarkedEntry ? 'true' : 'false');
        $('#subscribe-selected-rtdb-button').toggleClass('disabled', !hasMarkedEntry).attr('aria-disabled', !hasMarkedEntry ? 'true' : 'false');
    }

    async function handleSaveClick() {
        if (isSaving) {
            debugLog('handleSaveClick ignored because save is already running');
            return;
        }

        try {
            setSavingState(true);
            showToast('Speichern l\u00E4uft...');
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
        const activeRows = rows.filter((row) => row && row.sync !== false);
        const inactiveStateIds = rows
            .filter((row) => row && row.sync === false)
            .map((row) => row.stateId)
            .filter(Boolean);
        const currentStateIds = new Set(activeRows.map((row) => row.stateId));
        debugLog('saveRows start', {
            rows: rows.map((row) => ({
                stateId: row.stateId,
                sync: row.sync,
                key: row.key
            })),
            activeRows: activeRows.map((row) => row.stateId),
            inactiveStateIds,
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
        inactiveStateIds.forEach((stateId) => stateIdsToRemove.add(stateId));

        debugLog('saveRows removal reconciliation', {
            configuredStateIds,
            initiallyLoadedStateIds: [...initiallyLoadedStateIds],
            currentStateIds: [...currentStateIds],
            stateIdsToRemove: [...stateIdsToRemove]
        });

        debugLog('SAVE_ORDER_V3 instance-first writing instance object', {
            instanceObjectId,
            channelCount: activeRows.length
        });
        instanceObject.native.channels = activeRows.map((row) => ({
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
        const nextReadSubscriptionsByPath = new Map(savedReadSubscriptionsByPath);
        for (const entry of loadedReadableRtdbEntries) {
            if (!entry || !entry.path) {
                continue;
            }
            const path = String(entry.path).trim();
            if (!path) {
                continue;
            }
            if (selectedReadableRtdbPaths.has(path)) {
                const existing = nextReadSubscriptionsByPath.get(path);
                nextReadSubscriptionsByPath.set(path, {
                    path,
                    stateId: (existing && existing.stateId) ? String(existing.stateId) : defaultReadStateId(path),
                    enabled: true
                });
            } else {
                nextReadSubscriptionsByPath.delete(path);
            }
        }
        instanceObject.native.readSubscriptions = [...nextReadSubscriptionsByPath.values()]
            .sort((a, b) => String(a.path).localeCompare(String(b.path)));
        await setAnyObject(instanceObjectId, instanceObject);
        debugLog('SAVE_ORDER_V3 instance-first wrote instance object successfully', {
            instanceObjectId,
            channelCount: instanceObject.native.channels.length,
            readSubscriptions: instanceObject.native.readSubscriptions.length
        });
        savedReadSubscriptionsByPath = readSubscriptionsToMap(instanceObject.native.readSubscriptions);

        debugLog('saveRows applies direct state custom cleanup', {
            namespace,
            stateIdsToRemove: [...stateIdsToRemove],
            rowStateIds: rows.map((row) => row.stateId)
        });
        await removeStateCustomConfig([...stateIdsToRemove]);

        removedStateIds = new Set();
        initiallyLoadedStateIds = new Set(rows.map((row) => row.stateId));
        updateSummary(instanceObject, rows);
        renderRows();
        if (!silent) {
            showToast('Channels gespeichert. Objektkonfiguration wurde aktualisiert.');
        }
        setStatus(`Instanz ${namespace} gespeichert`);
        debugLog('saveRows finished successfully');
    }

    async function removeStateCustomConfig(stateIds) {
        for (const stateId of stateIds) {
            const object = await getAnyObject(stateId);
            if (!object || object.type !== 'state' || !object.common) {
                continue;
            }

            const customMap = object.common.custom || {};
            const namespaceCustom = customMap[namespace];
            const legacyCustom = customMap[ADAPTER];
            if (!namespaceCustom && !legacyCustom) {
                continue;
            }

            const nextObject = cloneObjectForWrite(object);
            nextObject.common = nextObject.common || {};
            nextObject.common.custom = nextObject.common.custom || {};
            const disabledCustom = {
                enabled: false,
                sync: false,
                key: (namespaceCustom && namespaceCustom.key) || (legacyCustom && legacyCustom.key) || objectIdToFirebaseKey(stateId),
                mode: (namespaceCustom && namespaceCustom.mode) || (legacyCustom && legacyCustom.mode) || DEFAULT_ROW.mode,
                minChange: (namespaceCustom && namespaceCustom.minChange) ?? (legacyCustom && legacyCustom.minChange) ?? DEFAULT_ROW.minChange,
                factor: (namespaceCustom && namespaceCustom.factor) ?? (legacyCustom && legacyCustom.factor) ?? DEFAULT_ROW.factor,
                transform: (namespaceCustom && namespaceCustom.transform) || (legacyCustom && legacyCustom.transform) || DEFAULT_ROW.transform,
                round: (namespaceCustom && namespaceCustom.round) ?? (legacyCustom && legacyCustom.round) ?? DEFAULT_ROW.round,
                minSendIntervalMs: (namespaceCustom && namespaceCustom.minSendIntervalMs) ?? (legacyCustom && legacyCustom.minSendIntervalMs) ?? DEFAULT_ROW.minSendIntervalMs,
                maxSendIntervalMs: (namespaceCustom && namespaceCustom.maxSendIntervalMs) ?? (legacyCustom && legacyCustom.maxSendIntervalMs) ?? DEFAULT_ROW.maxSendIntervalMs,
                dailyHour: normalizeDailyHour((namespaceCustom && namespaceCustom.dailyHour) ?? (legacyCustom && legacyCustom.dailyHour) ?? DEFAULT_ROW.dailyHour),
                dailyMinute: normalizeDailyMinute((namespaceCustom && namespaceCustom.dailyMinute) ?? (legacyCustom && legacyCustom.dailyMinute) ?? DEFAULT_ROW.dailyMinute),
                defaultValue: (namespaceCustom && namespaceCustom.defaultValue) ?? (legacyCustom && legacyCustom.defaultValue) ?? null
            };
            nextObject.common.custom[namespace] = disabledCustom;
            nextObject.common.custom[ADAPTER] = Object.assign({}, disabledCustom);

            await setAnyObject(stateId, nextObject);
        }
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

    function normalizeRtdbPath(value, fallback) {
        const normalized = String(value || '')
            .trim()
            .replace(/^\/+|\/+$/g, '');
        if (normalized) {
            return normalized;
        }
        return String(fallback || '')
            .trim()
            .replace(/^\/+|\/+$/g, '');
    }

    function relativeKeyFromPath(path) {
        const normalizedPath = normalizeRtdbPath(path, '');
        const basePath = normalizeRtdbPath($('#rtdb-read-path').val(), 'data');
        if (basePath && normalizedPath.startsWith(`${basePath}/`)) {
            return normalizedPath.slice(basePath.length + 1);
        }
        if (normalizedPath === basePath) {
            return '';
        }
        return normalizedPath;
    }

    function normalizeCreateType(value) {
        const normalized = String(value || '').trim().toLowerCase();
        if (normalized === 'boolean') {
            return 'bool';
        }
        if (['number', 'string', 'bool', 'array', 'object', 'mixed'].includes(normalized)) {
            return normalized;
        }
        return 'string';
    }

    function inferTypeFromValue(value) {
        if (typeof value === 'number') {
            return 'number';
        }
        if (typeof value === 'boolean') {
            return 'bool';
        }
        if (Array.isArray(value)) {
            return 'array';
        }
        if (value && typeof value === 'object') {
            return 'object';
        }
        if (value === null || value === undefined) {
            return 'string';
        }
        return 'string';
    }

    function stateValueToInput(type, value) {
        const normalizedType = normalizeCreateType(type);
        if (normalizedType === 'number') {
            return value === null || value === undefined ? '' : String(value);
        }
        if (normalizedType === 'bool') {
            return value ? 'true' : 'false';
        }
        if (normalizedType === 'array' || normalizedType === 'object') {
            try {
                return JSON.stringify(value);
            } catch {
                return '';
            }
        }
        if (normalizedType === 'mixed') {
            if (value === null || value === undefined) {
                return '';
            }
            if (typeof value === 'string') {
                return value;
            }
            try {
                return JSON.stringify(value);
            } catch {
                return String(value);
            }
        }
        return value === null || value === undefined ? '' : String(value);
    }

    function escapeSelectorValue(value) {
        return String(value || '').replace(/([!"#$%&'()*+,./:;<=>?@[\\\]^`{|}~])/g, '\\$1');
    }

    function parseCreateStateValue(type, raw) {
        const normalizedType = String(type || '').trim().toLowerCase();
        if (normalizedType === 'number') {
            const parsedNumber = Number(raw);
            return Number.isFinite(parsedNumber) ? parsedNumber : undefined;
        }
        if (normalizedType === 'bool' || normalizedType === 'boolean') {
            const lowered = String(raw || '').trim().toLowerCase();
            if (['true', '1', 'yes', 'ja', 'on'].includes(lowered)) {
                return true;
            }
            if (['false', '0', 'no', 'nein', 'off', ''].includes(lowered)) {
                return false;
            }
            return undefined;
        }
        if (normalizedType === 'array') {
            try {
                const parsed = JSON.parse(String(raw || '[]'));
                return Array.isArray(parsed) ? parsed : undefined;
            } catch {
                return undefined;
            }
        }
        if (normalizedType === 'object') {
            try {
                const parsed = JSON.parse(String(raw || '{}'));
                return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : undefined;
            } catch {
                return undefined;
            }
        }
        if (normalizedType === 'mixed') {
            const text = String(raw || '').trim();
            if (!text) {
                return '';
            }
            try {
                return JSON.parse(text);
            } catch {
                return text;
            }
        }
        return String(raw || '');
    }

    function readSubscriptionsToMap(value) {
        const list = Array.isArray(value) ? value : [];
        const map = new Map();
        list.forEach((item) => {
            const path = item && item.path ? String(item.path).trim().replace(/^\/+|\/+$/g, '') : '';
            if (!path) {
                return;
            }
            const stateId = item && item.stateId ? String(item.stateId).trim() : defaultReadStateId(path);
            const enabled = item && item.enabled !== false;
            if (!enabled) {
                return;
            }
            map.set(path, { path, stateId, enabled: true });
        });
        return map;
    }

    function defaultReadStateId(path) {
        const suffix = String(path || '')
            .split('/')
            .map((part) => part.trim().replace(/[^a-zA-Z0-9_-]+/g, '_').replace(/^_+|_+$/g, ''))
            .filter(Boolean)
            .join('.');
        return suffix ? `${namespace}.read.${suffix}` : `${namespace}.read.value`;
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

    function getStateUnit(object) {
        const unit = object?.common?.unit;
        return unit === null || unit === undefined ? '' : String(unit);
    }

    function formatUnit(unit) {
        const normalized = String(unit || '').trim();
        return normalized || '-';
    }

    function formatCurrentValueWithUnit(row) {
        const valueText = formatCurrentValue(row?.currentValue);
        const unitText = String(row?.unit || '').trim();
        return unitText && valueText !== '-' ? `${valueText} ${unitText}` : valueText;
    }

    function formatCurrentValue(value) {
        if (value === null || value === undefined) {
            return '-';
        }
        if (typeof value === 'object') {
            try {
                return JSON.stringify(value);
            } catch {
                return String(value);
            }
        }
        return String(value);
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

    function getAnyState(id) {
        const command = 'getState';
        return new Promise((resolve, reject) => {
            const startedAt = Date.now();
            const timeout = setTimeout(() => {
                reject(new Error(`${command} timeout after ${SOCKET_TIMEOUT_MS}ms for ${id}`));
            }, SOCKET_TIMEOUT_MS);

            socket.emit(command, id, (err, state) => {
                clearTimeout(timeout);
                if (err) {
                    reject(new Error(String(err)));
                    return;
                }

                debugLog('socket state completed', {
                    command,
                    id,
                    durationMs: Date.now() - startedAt
                });
                resolve(state);
            });
        });
    }

    function setAnyObject(id, obj) {
        const command = 'setObject';
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


