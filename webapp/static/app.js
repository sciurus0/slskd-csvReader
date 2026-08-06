(() => {
  const statusBody = document.getElementById("status-body");
  const logBody = document.getElementById("log-body");
  const message = document.getElementById("message");
  const buttons = document.querySelectorAll("button[data-action]");
  const playlistList = document.getElementById("playlist-list");
  const playlistToolbar = document.querySelectorAll("button[data-playlist-action]");
  const pipelineButtons = document.querySelectorAll(
    'button[data-action="pipeline"], button[data-action="pipeline-dry"]'
  );

  let pollTimer = null;
  let playlists = [];

  function showMessage(text, kind) {
    message.hidden = !text;
    message.textContent = text || "";
    message.className = "message" + (kind ? " " + kind : "");
  }

  async function refreshStatus() {
    const res = await fetch("/api/status");
    if (!res.ok) throw new Error("status " + res.status);
    const data = await res.json();
    const saved = data.saved_playlists || {};
    const lines = [
      "workspace: " + data.workspace,
      "slskd:     " + data.slskd_base_url,
      "queue:     " + (data.queue_rows == null ? "(missing)" : data.queue_rows + " rows"),
      "ledger:    " + (data.ledger_rows == null ? "(missing)" : data.ledger_rows + " rows"),
      "saved:     " + (saved.enabled || 0) + "/" + (saved.count || 0) + " enabled",
    ];
    if (saved.names && saved.names.length) {
      lines.push("playlists: " + saved.names.join(", "));
    }
    if (data.active_run) {
      lines.push("active:    " + data.active_run.action + " (" + data.active_run.id + ")");
    } else {
      lines.push("active:    none");
    }
    statusBody.textContent = lines.join("\n");

    const focus = data.active_run || (data.recent_runs && data.recent_runs[0]);
    if (focus) {
      await refreshLog(focus.id);
      if (focus.status === "running") {
        schedulePoll(focus.id);
      } else {
        clearPoll();
      }
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
    try {
      await refreshStatus();
    } catch (_) {
      /* ignore */
    }
  }

  function schedulePoll(runId) {
    clearPoll();
    pollTimer = setInterval(() => {
      refreshLog(runId).catch(() => {});
    }, 2000);
  }

  function clearPoll() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  function setBusy(busy) {
    buttons.forEach((btn) => {
      if (btn.dataset.action === "refresh") return;
      btn.disabled = busy;
    });
    // Re-apply the "needs >=1 enabled playlist" constraint on top of busy-state,
    // since clearing busy alone would otherwise re-enable pipeline buttons.
    updatePipelineButtons();
    playlistList.querySelectorAll("input[type=checkbox]").forEach((cb) => {
      cb.disabled = busy;
    });
    playlistToolbar.forEach((btn) => {
      btn.disabled = busy;
    });
  }

  function updatePipelineButtons() {
    const anyEnabled = playlists.some((pl) => pl.enabled);
    pipelineButtons.forEach((btn) => {
      btn.disabled = btn.disabled || !anyEnabled;
      btn.title = anyEnabled ? "" : "Enable at least one saved playlist first";
    });
  }

  function renderPlaylists() {
    if (!playlists.length) {
      playlistList.innerHTML =
        '<li class="muted">No saved playlists yet. Use the CLI (`--pick`) to add some.</li>';
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
      input.addEventListener("change", () => togglePlaylist(pl.id, input.checked));

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

  async function loadPlaylists() {
    try {
      const res = await fetch("/api/playlists");
      if (!res.ok) throw new Error("playlists " + res.status);
      const data = await res.json();
      playlists = data.playlists || [];
      renderPlaylists();
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
    renderPlaylists();
    await refreshStatusQuiet();
  }

  function togglePlaylist(id, enabled) {
    savePlaylistUpdates([{ id, enabled }]);
  }

  async function start(path, body) {
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

  buttons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const action = btn.dataset.action;
      if (action === "refresh") {
        refreshStatus().catch((e) => showMessage(String(e), "error"));
        return;
      }
      if (action === "pipeline") start("/api/runs/pipeline", { dry_run: false });
      if (action === "pipeline-dry") start("/api/runs/pipeline", { dry_run: true });
      if (action === "reconcile") start("/api/runs/reconcile");
      if (action === "trim") start("/api/runs/trim", { dry_run: false });
    });
  });

  playlistToolbar.forEach((btn) => {
    btn.addEventListener("click", () => {
      const enabled = btn.dataset.playlistAction === "select-all";
      if (!playlists.length) return;
      savePlaylistUpdates(playlists.map((pl) => ({ id: pl.id, enabled })));
    });
  });

  refreshStatus().catch((e) => {
    statusBody.textContent = String(e);
    showMessage(String(e), "error");
  });
  loadPlaylists();
})();
