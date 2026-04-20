(function () {
    const ADAPTER = 'firebase-history-sync';
    const DEFAULT_ROW = {
        sync: true,
        mode: 'threshold',
        minChange: 0,
        factor: 1,
        transform: 'none',
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
    let availableStates = [];
    let pickerSelection = new Set();
    let pendingDeleteIndex = null;
    let skipDeleteConfirmForSession = false;

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

        socket = window.io.connect();
        instance = detectInstance();
        namespace = `${ADAPTER}.${instance}`;
        instanceObjectId = `system.adapter.${namespace}`;

        bindUi();
        setStatus(`Instanz ${namespace}`);
        await loadPage();
    }

    function bindUi() {
        $('#reload-button').off('click').on('click', () => void loadPage());
        $('#save-button').off('click').on('click', () => void saveRows());
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

        const instanceObject = await getAnyObject(instanceObjectId);
        if (!instanceObject) {
            throw new Error(`Instanzobjekt nicht gefunden: ${instanceObjectId}`);
        }

        updateSummary(instanceObject, []);

        const stateView = await getObjectView('system', 'state', {
            startkey: '',
            endkey: '\u9999'
        });

        availableStates = (stateView.rows || [])
            .map((row) => row?.id || row?.value?._id)
            .filter(Boolean)
            .sort((a, b) => String(a).localeCompare(String(b)));

        rows = (stateView.rows || [])
            .map((row) => mapRowToChannel(row))
            .filter(Boolean)
            .sort((a, b) => String(a.stateId).localeCompare(String(b.stateId)));

        renderRows();
        updateSummary(instanceObject, rows);
    }

    async function openPicker() {
        if (!availableStates.length) {
            await loadPage();
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
                round: DEFAULT_ROW.round,
                minSendIntervalMs: DEFAULT_ROW.minSendIntervalMs,
                maxSendIntervalMs: DEFAULT_ROW.maxSendIntervalMs,
                defaultValue: DEFAULT_ROW.defaultValue
            });
        }

        rows.sort((a, b) => a.stateId.localeCompare(b.stateId));
        renderRows();
        updateSummaryDisplay(rows);
        await saveRows(true);
        $('#object-picker-modal').modal('close');
        showToast(`${selectedStateIds.length} Datenpunkte gespeichert.`);
    }

    function mapRowToChannel(row) {
        const object = row?.value;
        const stateId = row?.id || object?._id;
        const custom = object?.common?.custom?.[namespace];
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
            round: custom.round ?? DEFAULT_ROW.round,
            minSendIntervalMs: custom.minSendIntervalMs ?? DEFAULT_ROW.minSendIntervalMs,
            maxSendIntervalMs: custom.maxSendIntervalMs ?? DEFAULT_ROW.maxSendIntervalMs,
            defaultValue: custom.defaultValue ?? ''
        };
    }

    function renderRows(emptyMessage) {
        const $body = $('#channel-table-body');
        $body.empty();

        if (!rows.length) {
            $body.append(`<div class="empty-row">${emptyMessage || 'Keine Datenpunkte ausgewaehlt.'}</div>`);
            return;
        }

        rows.forEach((row, index) => {
            const $card = $(`
                <div class="channel-card" data-index="${index}">
                    <div class="channel-summary">
                        <div class="summary-sync">
                            ${renderCheckbox(row.sync)}
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
                            <div class="detail-field full">
                                <label>State ID</label>
                                <input class="state-id" type="text" data-field="stateId" value="${escapeHtml(row.stateId)}" />
                            </div>
                            <div class="detail-field full">
                                <label>Firebase Key</label>
                                <input class="channel-key" type="text" data-field="key" value="${escapeHtml(row.key)}" />
                            </div>
                            <div class="detail-field">
                                <label>Mode</label>
                                ${renderSelect('mode', row.mode, ['threshold', 'change', 'daily_only'])}
                            </div>
                            <div class="detail-field">
                                <label>minChange</label>
                                <input type="number" step="0.1" data-field="minChange" value="${row.minChange}" />
                            </div>
                            <div class="detail-field">
                                <label>Factor</label>
                                <input type="number" step="0.1" data-field="factor" value="${row.factor}" />
                            </div>
                            <div class="detail-field">
                                <label>Transform</label>
                                ${renderSelect('transform', row.transform, ['none', 'positive_only', 'boolean'])}
                            </div>
                            <div class="detail-field">
                                <label>Round</label>
                                <input type="number" step="1" data-field="round" value="${row.round}" />
                            </div>
                            <div class="detail-field">
                                <label>Min ms</label>
                                <input type="number" step="1" data-field="minSendIntervalMs" value="${row.minSendIntervalMs}" />
                            </div>
                            <div class="detail-field">
                                <label>Max ms</label>
                                <input type="number" step="1" data-field="maxSendIntervalMs" value="${row.maxSendIntervalMs}" />
                            </div>
                            <div class="detail-field">
                                <label>Default</label>
                                <input type="number" step="0.1" data-field="defaultValue" value="${row.defaultValue}" />
                            </div>
                        </div>
                        <div class="detail-actions">
                            <button class="delete-button" title="Channel entfernen">Datenpunkt loeschen</button>
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
            $card.find('[data-field]').on('change keyup', () => syncRowFromDom($card));
            $card.find('.delete-button').on('click', () => confirmRemoveRow(index));
            $card.find('[data-field="sync"]').on('click', (event) => event.stopPropagation());
            $body.append($card);
        });
    }

    function renderCheckbox(value) {
        return `<label><input type="checkbox" data-field="sync" ${value ? 'checked' : ''} /><span></span></label>`;
    }

    function renderSelect(field, value, options) {
        const optionHtml = options
            .map((item) => `<option value="${item}" ${item === value ? 'selected' : ''}>${item}</option>`)
            .join('');
        return `<select data-field="${field}">${optionHtml}</select>`;
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
        row.round = toNumber($tr.find('[data-field="round"]').val(), DEFAULT_ROW.round);
        row.minSendIntervalMs = toNumber($tr.find('[data-field="minSendIntervalMs"]').val(), DEFAULT_ROW.minSendIntervalMs);
        row.maxSendIntervalMs = toNumber($tr.find('[data-field="maxSendIntervalMs"]').val(), DEFAULT_ROW.maxSendIntervalMs);
        row.defaultValue = normalizeDefaultValue($tr.find('[data-field="defaultValue"]').val());

        const $card = $tr.closest('.channel-card');
        $card.find('.summary-field .summary-value-inline').eq(0).text(row.stateId);
        $card.find('.summary-field .summary-value-inline').eq(1).text(row.key);
        updateSummaryDisplay(rows);
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
        showToast('Datenpunkt aus der Liste entfernt.');
        void saveRows(true).catch((error) => {
            showToast(`Loeschen fehlgeschlagen: ${error.message}`);
        });
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

        rows.push({
            stateId,
            key: objectIdToFirebaseKey(stateId),
            sync: true,
            mode: DEFAULT_ROW.mode,
            minChange: DEFAULT_ROW.minChange,
            factor: DEFAULT_ROW.factor,
            transform: DEFAULT_ROW.transform,
            round: DEFAULT_ROW.round,
            minSendIntervalMs: DEFAULT_ROW.minSendIntervalMs,
            maxSendIntervalMs: DEFAULT_ROW.maxSendIntervalMs,
            defaultValue: DEFAULT_ROW.defaultValue
        });

        rows.sort((a, b) => a.stateId.localeCompare(b.stateId));
        $('#new-state-id').val('');
        renderRows();
        updateSummaryDisplay(rows);
        showToast('State-ID hinzugefuegt.');
        await saveRows(true);
    }

    async function saveRows(silent) {
        const instanceObject = await getAnyObject(instanceObjectId);
        if (!instanceObject) {
            throw new Error(`Instanzobjekt nicht gefunden: ${instanceObjectId}`);
        }

        const rowMap = new Map(rows.map((row) => [row.stateId, normalizeRow(row)]));

        for (const stateId of removedStateIds) {
            const object = await getAnyObject(stateId);
            if (!object?.common) {
                continue;
            }

            object.common.custom = object.common.custom || {};
            if (object.common.custom[namespace]) {
                delete object.common.custom[namespace];
                await setAnyObject(stateId, object);
            }
        }

        for (const row of rows) {
            const object = await getAnyObject(row.stateId);
            if (!object?.common) {
                throw new Error(`State object not found: ${row.stateId}`);
            }

            object.common.custom = object.common.custom || {};
            object.common.custom[namespace] = rowMap.get(row.stateId);
            await setAnyObject(row.stateId, object);
        }

        instanceObject.native.channels = rows.map((row) => ({
            key: row.key,
            stateId: row.stateId,
            enabled: true,
            sync: row.sync,
            mode: row.mode,
            minChange: row.minChange,
            factor: row.factor,
            transform: row.transform,
            round: row.round,
            minSendIntervalMs: row.minSendIntervalMs,
            maxSendIntervalMs: row.maxSendIntervalMs,
            defaultValue: normalizeDefaultValue(row.defaultValue)
        }));
        await setAnyObject(instanceObjectId, instanceObject);

        removedStateIds = new Set();
        if (!silent) {
            showToast('Channels gespeichert. Bitte den Adapter neu starten.');
        }
        await loadPage();
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
            round: row.round,
            minSendIntervalMs: row.minSendIntervalMs,
            maxSendIntervalMs: row.maxSendIntervalMs,
            defaultValue: normalizeDefaultValue(row.defaultValue)
        };
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
        return `custom.${objectId.replace(/[^a-zA-Z0-9]+/g, '_').replace(/^_+|_+$/g, '')}`;
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

    function showToast(message) {
        if (window.M && M.toast) {
            M.toast({ html: message, displayLength: 4000 });
        } else {
            console.error(message);
        }
    }

    function getAnyObject(id) {
        const command = isSystemObject(id) ? 'getObject' : 'getForeignObject';
        return new Promise((resolve, reject) => {
            socket.emit(command, id, (err, obj) => {
                if (err) {
                    reject(new Error(String(err)));
                    return;
                }

                resolve(obj);
            });
        });
    }

    function setAnyObject(id, obj) {
        const command = isSystemObject(id) ? 'setObject' : 'setForeignObject';
        return new Promise((resolve, reject) => {
            socket.emit(command, id, obj, (err) => {
                if (err) {
                    reject(new Error(String(err)));
                    return;
                }

                resolve();
            });
        });
    }

    function isSystemObject(id) {
        return String(id).startsWith('system.');
    }

    function getObjectView(design, search, params) {
        return new Promise((resolve, reject) => {
            socket.emit('getObjectView', design, search, params, (err, result) => {
                if (err) {
                    reject(new Error(String(err)));
                    return;
                }

                resolve(result);
            });
        });
    }
})();
