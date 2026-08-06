(() => {
  const statusBody = document.getElementById("status-body");
  const logBody = document.getElementById("log-body");
  const message = document.getElementById("message");
  const playlistList = document.getElementById("playlist-list");
  const libraryList = document.getElementById("library-list");
  const authBanner = document.getElementById("auth-banner");
  const authDetail = document.getElementById("auth-detail");
  const reauthCommand = document.getElementById("reauth-command");
  const runButtons = () => document.querySelectorAll(
    "button[id^='btn-'], button[data-action], button[data-playlist-action], #refresh-library, #save-library-selection, #library-select-all, #library-select-none, #copy-reauth"
  );

  let pollTimer = null;
  let playlists = [];
  let library = [];
  let busy = false;

  function showMessage(text, kind) {
    message.hidden = !text;
    message.textContent = text || "";
    message.className = "message" + (kind ? " " + kind : "");
  }

  function numOrNull(id) {
    const el = document.getElementById(id);
    if (!el || el.value === "" || el.value == null) return null;
    const n = Number(el.value);
    return Number.isFinite(n) ? n : null;
  }

  function val(id) {
    const el = document.getElementById(id);
    return el ? (el.value || "").trim() : "";
  }

  function checked(id) {
    const el = document.getElementById(id);
    return !!(el && el.checked);
  }

  function selectionMode() {
    const el = document.querySelector('input[name="selection"]:checked');
    return el ? el.value : "saved";
  }

  function confirmLive(label) {
    return window.confirm("Run “" + label + "”? This may change queue/ledger/checkpoint state.");
  }

  function setBusy(isBusy) {
    busy = isBusy;
    runButtons().forEach((btn) => {
      if (btn.dataset.action === "refresh" || btn.id === "copy-reauth") return;
      if (btn.classList.contains("tab")) return;
      btn.disabled = isBusy;
    });
    playlistList.querySelectorAll("input").forEach((cb) => { cb.disabled = isBusy; });
    libraryList.querySelectorAll("input").forEach((cb) => { cb.disabled = isBusy; });
    updatePipelineButtons();
  }

  function updatePipelineButtons() {
    const anyEnabled = playlists.some((pl) => pl.enabled);
    const sel = selectionMode();
    const needsSaved = sel === "saved" || sel === "saved_indices";
    ["btn-pipeline", "btn-pipeline-dry"].forEach((id) => {
      const btn = document.getElementById(id);
      if (!btn) return;
      const block = busy || (needsSaved && !anyEnabled && sel === "saved");
      btn.disabled = block;
      btn.title = block && !busy && needsSaved ? "Enable at least one saved playlist" : "";
    });
  }

  function renderAuth(token) {
    if (!token) {
      authBanner.hidden = true;
      return;
    }
    if (token.ok) {
      authBanner.hidden = true;
      return;
    }
    authBanner.hidden = false;
    authDetail.textContent = token.detail || "Spotify tokens unavailable";
    reauthCommand.textContent = token.reauth_command || "NAS_HOST=nas bash scripts/nas-spotify-reauth.sh";
  }

  async function refreshStatus() {
    const res = await fetch("/api/status");
    if (!res.ok) throw new Error("status " + res.status);
    const data = await res.json();
    const saved = data.saved_playlists || {};
    const ckpt = data.checkpoint || {};
    const token = data.spotify_token || {};
    const lines = [
      "workspace:  " + data.workspace,
      "slskd:      " + data.slskd_base_url,
      "queue:      " + (data.queue_rows == null ? "(missing)" : data.queue_rows + " rows"),
      "ledger:     " + (data.ledger_rows == null ? "(missing)" : data.ledger_rows + " rows"),
      "saved:      " + (saved.enabled || 0) + "/" + (saved.count || 0) + " enabled",
      "checkpoint: " + (ckpt.exists ? "yes (" + ckpt.size + " B)" : "none"),
      "spotify:    " + (token.ok ? "ok" : "NEEDS REAUTH") + " — " + (token.detail || ""),
    ];
    if (saved.names && saved.names.length) {
      lines.push("playlists:  " + saved.names.join(", "));
    }
    if (data.active_run) {
      lines.push("active:     " + data.active_run.action + " (" + data.active_run.id + ")");
    } else {
      lines.push("active:     none");
    }
    statusBody.textContent = lines.join("\n");
    renderAuth(token);

    const focus = data.active_run || (data.recent_runs && data.recent_runs[0]);
    if (focus) {
      await refreshLog(focus.id);
      if (focus.status === "running") schedulePoll(focus.id);
      else clearPoll();
    } else {
      logBody.textContent = "(none)";
      clearPoll();
    }
    setBusy(!!data.active_run);
  }

  async function refreshLog(runId) {
    const res = await fetch("/api/runs/" + encodeURIComponent(runId));
    if (!res.ok) return;
    const data = await res.json();
    const header =
      "[" + data.status + "] " + data.action + " " + data.id +
      (data.returncode != null ? " exit=" + data.returncode : "") +
      "\n\n";
    logBody.textContent = header + (data.log_tail || "(empty log)");
    if (data.status !== "running") {
      clearPoll();
      setBusy(false);
      showMessage(
        data.status === "succeeded" ? "Run finished OK" : "Run failed (see log)",
        data.status === "succeeded" ? "ok" : "error"
      );
      await refreshStatusQuiet();
    }
  }

  async function refreshStatusQuiet() {
    try { await refreshStatus(); } catch (_) { /* ignore */ }
  }

  function schedulePoll(runId) {
    clearPoll();
    pollTimer = setInterval(() => { refreshLog(runId).catch(() => {}); }, 2000);
  }

  function clearPoll() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  function renderSaved() {
    if (!playlists.length) {
      playlistList.innerHTML =
        '<li class="muted">No saved playlists. Refresh Library from Spotify and save a selection.</li>';
      updatePipelineButtons();
      return;
    }
    playlistList.innerHTML = "";
    playlists.forEach((pl) => {
      const li = document.createElement("li");
      li.className = "playlist-row";
      const label = document.createElement("label");
      label.className = "switch-row";
      const input = document.createElement("input");
      input.type = "checkbox";
      input.checked = !!pl.enabled;
      input.addEventListener("change", () => savePlaylistUpdates([{ id: pl.id, enabled: input.checked }]));
      const track = document.createElement("span");
      track.className = "switch-track";
      const name = document.createElement("span");
      name.className = "playlist-name";
      name.textContent = pl.name || pl.id;
      label.appendChild(input);
      label.appendChild(track);
      label.appendChild(name);
      li.appendChild(label);
      playlistList.appendChild(li);
    });
    updatePipelineButtons();
  }

  function renderLibrary() {
    if (!library.length) {
      libraryList.innerHTML = '<li class="muted">No library playlists loaded.</li>';
      return;
    }
    libraryList.innerHTML = "";
    library.forEach((pl) => {
      const li = document.createElement("li");
      li.className = "playlist-row";
      const label = document.createElement("label");
      label.className = "library-check";
      const input = document.createElement("input");
      input.type = "checkbox";
      input.dataset.id = pl.id;
      input.dataset.name = pl.name || "";
      const name = document.createElement("span");
      name.className = "playlist-name";
      const tracks = pl.tracks_total != null && pl.tracks_total >= 0 ? " (" + pl.tracks_total + ")" : "";
      name.textContent = (pl.index ? pl.index + ". " : "") + (pl.name || pl.id) + tracks;
      label.appendChild(input);
      label.appendChild(name);
      li.appendChild(label);
      libraryList.appendChild(li);
    });
  }

  async function loadPlaylists() {
    try {
      const res = await fetch("/api/playlists");
      if (!res.ok) throw new Error("playlists " + res.status);
      const data = await res.json();
      playlists = data.playlists || [];
      renderSaved();
    } catch (e) {
      playlistList.innerHTML = '<li class="muted">Failed to load playlists.</li>';
    }
  }

  async function savePlaylistUpdates(updates) {
    const res = await fetch("/api/playlists", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ updates }),
    });
    if (!res.ok) {
      showMessage("Failed to update playlist selection", "error");
      return;
    }
    const data = await res.json();
    playlists = data.playlists || [];
    renderSaved();
    await refreshStatusQuiet();
  }

  async function start(path, body, { confirmLabel } = {}) {
    if (confirmLabel && !confirmLive(confirmLabel)) return;
    showMessage("Starting…");
    setBusy(true);
    const res = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body ? JSON.stringify(body) : "{}",
    });
    const data = await res.json().catch(() => ({}));
    if (res.status === 409) {
      showMessage(data.detail || "Busy", "error");
      setBusy(true);
      await refreshStatus();
      return;
    }
    if (!res.ok) {
      showMessage(data.detail || ("HTTP " + res.status), "error");
      setBusy(false);
      return;
    }
    showMessage("Started " + data.action + " (" + data.id + ")");
    schedulePoll(data.id);
    await refreshLog(data.id);
  }

  function pipelineBody(forceDry) {
    const settle = numOrNull("opt-settle");
    return {
      selection: selectionMode(),
      saved_indices: val("saved-indices"),
      pick: val("pick-indices"),
      playlist_id: val("playlist-ids"),
      dry_run: forceDry || checked("opt-dry-run"),
      skip_slskd: checked("opt-skip-slskd"),
      force_full_import: checked("opt-force-full"),
      continue_on_export_error: checked("opt-continue-export"),
      no_save_picks: checked("opt-no-save-picks"),
      date: val("opt-date"),
      download_settle_seconds: settle,
      skip_pending_csv: checked("opt-skip-pending"),
      no_trim_queue: checked("opt-no-trim"),
      csv: val("opt-csv"),
      checkpoint_file: val("opt-checkpoint"),
    };
  }

  function tuningBody(extra) {
    return Object.assign({
      batch_size: numOrNull("tune-batch"),
      delay: numOrNull("tune-delay"),
      formats: val("tune-formats"),
      exclude: val("tune-exclude"),
      queue_limit: numOrNull("tune-queue-limit"),
      download_settle_seconds: numOrNull("tune-settle"),
      debug: checked("tune-debug"),
      skip_pending_csv: checked("tune-skip-pending"),
      no_trim_queue: checked("tune-no-trim"),
    }, extra || {});
  }

  // Tabs
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((t) => {
        t.classList.toggle("active", t === tab);
        t.setAttribute("aria-selected", t === tab ? "true" : "false");
      });
      const name = tab.dataset.tab;
      document.getElementById("tab-saved").hidden = name !== "saved";
      document.getElementById("tab-library").hidden = name !== "library";
    });
  });

  document.querySelectorAll("button[data-playlist-action]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const enabled = btn.dataset.playlistAction === "select-all";
      if (!playlists.length) return;
      savePlaylistUpdates(playlists.map((pl) => ({ id: pl.id, enabled })));
    });
  });

  document.getElementById("refresh-library").addEventListener("click", async () => {
    showMessage("Fetching Spotify library…");
    try {
      const res = await fetch("/api/spotify/library");
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        showMessage(data.detail || ("HTTP " + res.status), "error");
        await refreshStatusQuiet();
        return;
      }
      library = data.playlists || [];
      renderLibrary();
      showMessage("Loaded " + library.length + " playlist(s) from Spotify", "ok");
    } catch (e) {
      showMessage(String(e), "error");
    }
  });

  document.getElementById("library-select-all").addEventListener("click", () => {
    libraryList.querySelectorAll("input[type=checkbox]").forEach((cb) => { cb.checked = true; });
  });
  document.getElementById("library-select-none").addEventListener("click", () => {
    libraryList.querySelectorAll("input[type=checkbox]").forEach((cb) => { cb.checked = false; });
  });

  document.getElementById("save-library-selection").addEventListener("click", async () => {
    const selected = [];
    libraryList.querySelectorAll("input[type=checkbox]:checked").forEach((cb) => {
      selected.push({ id: cb.dataset.id, name: cb.dataset.name || "" });
    });
    if (!selected.length) {
      showMessage("Select at least one library playlist", "error");
      return;
    }
    const res = await fetch("/api/playlists", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        selected,
        replace_enabled_set: checked("replace-enabled-set"),
      }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      showMessage(data.detail || "Save failed", "error");
      return;
    }
    playlists = data.playlists || [];
    renderSaved();
    showMessage("Saved " + selected.length + " playlist(s) into saved_playlists.json", "ok");
    await refreshStatusQuiet();
    document.querySelector('.tab[data-tab="saved"]').click();
  });

  document.getElementById("copy-reauth").addEventListener("click", async () => {
    const text = reauthCommand.textContent || "";
    try {
      await navigator.clipboard.writeText(text);
      showMessage("Copied re-auth command", "ok");
    } catch (_) {
      showMessage("Copy failed — select the command manually", "error");
    }
  });

  document.getElementById("btn-pipeline").addEventListener("click", () => {
    const body = pipelineBody(false);
    const label = body.dry_run ? "Dry-run pipeline" : "Run pipeline";
    start("/api/runs/pipeline", body, { confirmLabel: body.dry_run ? null : label });
  });
  document.getElementById("btn-pipeline-dry").addEventListener("click", () => {
    start("/api/runs/pipeline", pipelineBody(true));
  });

  document.querySelector('button[data-action="refresh"]').addEventListener("click", () => {
    refreshStatus().catch((e) => showMessage(String(e), "error"));
  });

  document.getElementById("btn-reconcile").addEventListener("click", () => {
    start("/api/runs/reconcile", {
      reconcile_from_csv: val("reconcile-csv"),
      reconcile_log: val("reconcile-log"),
    }, { confirmLabel: "Reconcile downloads" });
  });
  document.getElementById("btn-trim").addEventListener("click", () => {
    start("/api/runs/trim", {
      dry_run: false,
      no_backup: checked("trim-no-backup"),
    }, { confirmLabel: "Trim queue" });
  });
  document.getElementById("btn-trim-dry").addEventListener("click", () => {
    start("/api/runs/trim", { dry_run: true, no_backup: checked("trim-no-backup") });
  });
  document.getElementById("btn-merge").addEventListener("click", () => {
    start("/api/runs/merge", {
      dry_run: false,
      force_full_import: checked("merge-force"),
      date: val("merge-date"),
    }, { confirmLabel: "Merge only" });
  });
  document.getElementById("btn-merge-dry").addEventListener("click", () => {
    start("/api/runs/merge", {
      dry_run: true,
      force_full_import: checked("merge-force"),
      date: val("merge-date"),
    });
  });
  document.getElementById("btn-retry").addEventListener("click", () => {
    start("/api/runs/slskd", tuningBody({ retry_failed: true }), { confirmLabel: "Retry failed" });
  });
  document.getElementById("btn-gen-report").addEventListener("click", () => {
    start("/api/runs/slskd", tuningBody({ gen_report: true }));
  });
  document.getElementById("btn-cleanup-validate").addEventListener("click", () => {
    start("/api/runs/cleanup", { validate_only: true });
  });
  document.getElementById("btn-cleanup-ephemeral").addEventListener("click", () => {
    start("/api/runs/cleanup", { ephemeral: true }, { confirmLabel: "Cleanup ephemeral CSVs" });
  });
  document.getElementById("btn-slskd-only").addEventListener("click", () => {
    start("/api/runs/slskd", tuningBody({}), { confirmLabel: "SLSKD process queue" });
  });
  document.getElementById("btn-resume").addEventListener("click", () => {
    start("/api/runs/slskd", tuningBody({ resume: true }), { confirmLabel: "Resume from checkpoint" });
  });

  document.querySelectorAll('input[name="selection"]').forEach((el) => {
    el.addEventListener("change", updatePipelineButtons);
  });

  refreshStatus().catch((e) => {
    statusBody.textContent = String(e);
    showMessage(String(e), "error");
  });
  loadPlaylists();
})();
