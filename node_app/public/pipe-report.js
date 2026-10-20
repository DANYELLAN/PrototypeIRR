(() => {
  let refreshing = false;
  let verifying = false;
  let revision = 0;
  const localTimes = () => document.querySelectorAll("#pipe-report-content time[datetime]").forEach((element) => { element.textContent = new Date(element.dateTime).toLocaleString(); });
  localTimes();
  const refresh = async () => {
    const current = document.getElementById("pipe-report-content");
    if (!current || refreshing || verifying || document.hidden || document.activeElement?.closest?.("[data-verify-inspection]")) return;
    refreshing = true;
    const requestedRevision = revision;
    try {
      const query = new URLSearchParams({ page: current.dataset.page, station: current.dataset.station });
      const response = await fetch(`/workflow/pipe-report/live.json?${query}`, { cache: "no-store" });
      if (!response.ok) throw new Error(response.status === 401 || response.status === 403 ? "Report access expired. Sign in again." : "Live refresh unavailable; the displayed report may be out of date.");
      const payload = await response.json();
      if (verifying || requestedRevision !== revision) return;
      const scrollPositions = new Map([...current.querySelectorAll("[data-grid-key]")].map((grid) => [grid.dataset.gridKey, grid.scrollLeft]));
      const next = new DOMParser().parseFromString(payload.html, "text/html").getElementById("pipe-report-content");
      if (!next) throw new Error("Live refresh unavailable; the displayed report may be out of date.");
      current.replaceWith(next);
      next.querySelectorAll("[data-grid-key]").forEach((grid) => { grid.scrollLeft = scrollPositions.get(grid.dataset.gridKey) || 0; });
      localTimes();
      document.getElementById("pipe-report-error").textContent = "";
      document.getElementById("pipe-report-freshness").textContent = `Updated ${new Date(payload.updated_at).toLocaleTimeString()} · refreshes every 5 seconds`;
    } catch (error) { document.getElementById("pipe-report-error").textContent = error.message; }
    finally { refreshing = false; }
  };
  document.addEventListener("submit", async (event) => {
    if (!event.target.matches("[data-verify-inspection]")) return;
    event.preventDefault();
    if (verifying) return;
    verifying = true;
    revision++;
    const button = event.target.querySelector("button");
    button.disabled = true;
    let saved = false;
    try {
      const response = await fetch(event.target.action, { method: "POST", body: new URLSearchParams(new FormData(event.target)) });
      if (response.status === 401 || response.status === 403) throw new Error("Verification access expired. Sign in again.");
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Verification was not saved. Refresh and try again.");
      saved = true;
      button.textContent = "Verified";
      verifying = false;
      button.blur();
      await refresh();
    } catch (error) { document.getElementById("pipe-report-error").textContent = error.message; }
    finally { verifying = false; if (button.isConnected) button.disabled = saved; }
  });
  window.setInterval(refresh, 5000);
  document.addEventListener("visibilitychange", () => { if (!document.hidden) void refresh(); });
})();
