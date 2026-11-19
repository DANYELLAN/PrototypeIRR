(() => {
  const root = document.querySelector("[data-admin-monitor]");
  if (!root) return;
  const escape = value => String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll('"', "&quot;");
  const timestamp = value => value ? new Date(value).toLocaleString(undefined, { timeZone: "America/Chicago" }) : "No readings received";
  const status = root.querySelector("[data-monitor-status]");
  const live = root.querySelector("[data-live-inspections]");
  const lunches = root.querySelector("[data-admin-lunch-records]");
  const filters = root.querySelector("[data-lunch-filters]");
  const automatic = root.querySelector("[data-live-updates]");
  let timer;
  let busy = false;
  let authorized = true;
  let signature = "";
  const refresh = async (manual = false) => {
    clearTimeout(timer);
    if (busy) return;
    if (document.hidden && !manual) { timer = setTimeout(refresh, 4000); return; }
    busy = true;
    const finish = manual ? window.inspectionLoading?.start("Loading inspection monitoring...") : null;
    try {
      const response = await fetch(`/admin/inspections/data?${new URLSearchParams(new FormData(filters))}`, { credentials: "same-origin", cache: "no-store" });
      const data = await response.json();
      if ([401, 403].includes(response.status)) authorized = false;
      if (!response.ok) throw Error(data.error || "Inspection monitoring could not update.");
      const nextSignature = JSON.stringify(data);
      if (nextSignature !== signature) {
        const expanded = new Set(Array.from(live.querySelectorAll("details[open]")).map(element => element.dataset.attemptId));
        live.innerHTML = data.live.length ? data.live.map(row => `<details class="live-inspection-row" data-attempt-id="${row.attempt_id}" ${expanded.has(String(row.attempt_id)) ? "open" : ""}>
          <summary><strong>${escape(row.location_name || "Unassigned machine")}</strong><span>${escape(row.branch)}</span><span>${escape(row.inspector_name)}${row.relief_for_inspector_name ? " (Relief)" : ""}</span><span>WO ${escape(row.production_number)} / Pipe ${escape(row.pipe_number)}</span><span>${row.entered} / ${row.required} entered</span><span>Last reading: ${escape(timestamp(row.draft_updated_at))}</span></summary>
          <p>${escape(row.operation_description)} &middot; ${escape(row.inspection_scope)} &middot; ${escape(row.shift)}</p>
          <div class="table-wrap"><table><thead><tr><th>#</th><th>Element</th><th>DWG DIM</th><th>Gauge</th><th>Current Reading</th><th>Check</th></tr></thead><tbody>${row.measurements.map(element => `<tr><td>${element.element_sequence}</td><td>${escape(element.element_description)}</td><td>${escape(element.dwg_dim)}</td><td>${escape(element.gauge)}</td><td>${escape(element.measured_value)}</td><td class="${element.pass_fail === "Fail" ? "lunch-fail" : element.pass_fail === "Pass" ? "lunch-pass" : ""}">${escape(element.pass_fail)}</td></tr>`).join("")}</tbody></table></div>
        </details>`).join("") : "<p>No ongoing local inspections.</p>";
        lunches.innerHTML = data.lunches.length ? data.lunches.map(row => `<tr><td>${escape(timestamp(row.submitted_at))}</td><td>${escape(row.branch)}</td><td>${escape(row.production_number)}</td><td>${escape(row.operation_description)}</td><td>${escape(row.pipe_number)}</td><td>${escape(row.inspector_name)}</td><td class="${row.result === "fail" ? "lunch-fail" : "lunch-pass"}">${escape(row.result.toUpperCase())}</td><td><button type="button" class="admin-record-link" data-lunch-view="${escape(row.id)}">View Comparison</button></td></tr>`).join("") : '<tr><td colspan="8">No submitted lunch inspections.</td></tr>';
        signature = nextSignature;
      }
      status.textContent = `Updated ${timestamp(new Date().toISOString())}`;
      status.setAttribute("role", "status");
    } catch (error) {
      status.textContent = `Update failed: ${error.message}`;
      status.setAttribute("role", "alert");
    } finally {
      finish?.();
      busy = false;
      if (automatic.checked && authorized) timer = setTimeout(refresh, 4000);
    }
  };
  filters.addEventListener("submit", event => { event.preventDefault(); refresh(true); });
  root.querySelector("[data-monitor-refresh]").addEventListener("click", () => refresh(true));
  automatic.addEventListener("change", () => { clearTimeout(timer); if (automatic.checked) refresh(); });
  document.addEventListener("visibilitychange", () => { if (!document.hidden && automatic.checked) refresh(); });
  refresh(true);
})();
