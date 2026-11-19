(() => {
  const escape = value => String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll('"', "&quot;");
  const timestamp = value => {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? String(value ?? "") : date.toLocaleString(undefined, { timeZone: "America/Chicago" });
  };
  window.initializeLunchInspection = (root = document) => {
    root.querySelectorAll("[data-lunch-dialog]").forEach(dialog => {
      if (dialog.dataset.initialized) return;
      dialog.dataset.initialized = "true";
      const content = dialog.querySelector("[data-lunch-content]");
      const openButton = dialog.closest("section").querySelector("[data-lunch-open]");
      const recordEndpoint = dialog.dataset.recordEndpoint || "/workflow/lunch/";
      let current = null;
      let csrf = "";
      let records = [];
      let busy = false;
      const request = async (url, options, message) => {
        const finish = window.inspectionLoading?.start(message);
        busy = true;
        try {
          const response = await fetch(url, { credentials: "same-origin", ...options });
          const data = await response.json();
          if (!response.ok) throw Error(data.error || "The lunch inspection could not be loaded.");
          return data;
        } finally { busy = false; finish?.(); }
      };
      const showError = message => {
        let error = content.querySelector("[data-lunch-error]");
        if (!error) {
          error = document.createElement("p");
          error.dataset.lunchError = "";
          error.setAttribute("role", "alert");
          content.prepend(error);
        }
        error.textContent = message;
      };
      const render = () => {
        const snapshot = current?.snapshot;
        const saved = Boolean(current?.submitted_at);
        const baseline = new Map((snapshot?.baseline || []).map(row => [Number(row.element_sequence), row]));
        const checked = new Map((current?.measurements || []).map(row => [Number(row.element_sequence), row]));
        content.innerHTML = `${snapshot ? `
          <p class="lunch-context"><strong>WO ${escape(snapshot.production_number)}</strong> &middot; Pipe # ${escape(snapshot.pipe_number)} &middot; ${escape(snapshot.operation_description)}</p>
          <p>Original inspector: ${escape(snapshot.inspector_name)} &middot; ${escape(timestamp(snapshot.completed_at))}</p>
          <form data-lunch-form>
            <div class="field lunch-adp-field"><label for="lunch-adp">Inspector ADP Number</label><input id="lunch-adp" name="adp" inputmode="numeric" autocomplete="off" required value="${saved ? escape(current.inspector_adp) : ""}" ${saved ? "readonly" : ""} /></div>
            ${saved ? `<p class="lunch-saved-result ${current.result === "fail" ? "lunch-fail" : "lunch-pass"}">Saved Lunch Inspection: ${escape(current.result.toUpperCase())} &middot; ${escape(current.inspector_name)}</p>` : ""}
            <p data-lunch-error role="alert"></p>
            <div class="table-wrap lunch-comparison-wrap"><table class="lunch-comparison-table">
              <thead><tr><th>#</th><th>Element</th><th>DWG DIM</th><th>Gauge</th><th>Pipe # ${escape(snapshot.pipe_number)}<br>Submitted</th><th>Pipe # ${escape(snapshot.pipe_number)}<br>Lunch Inspection</th></tr></thead>
              <tbody>${snapshot.elements.map(element => {
                const seq = Number(element.element_sequence);
                const value = checked.get(seq)?.measured_value ?? "";
                return `<tr><td>${seq}</td><td>${escape(element.element_description)}</td><td>${escape(element.dwg_dim)}</td><td>${escape(element.gauge)}</td><td>${escape(baseline.get(seq)?.measured_value ?? "Not recorded")}</td><td>${saved
                  ? `<span class="${checked.get(seq)?.pass_fail === "Fail" ? "lunch-fail" : "lunch-pass"}">${escape(value)}</span>`
                  : element.capture_type === "boolean"
                    ? `<select name="${seq}" data-lunch-reading data-sequence="${seq}" required><option value=""></option><option value="Yes">Pass</option><option value="No">Fail</option></select>`
                    : `<input data-measurement-offset data-nominal="${escape(element.nominal ?? '')}" name="${seq}" data-lunch-reading data-sequence="${seq}" inputmode="decimal" autocomplete="off" required />`}</td></tr>`;
              }).join("")}</tbody>
            </table></div>
            ${!saved ? `<div class="lunch-save-row"><strong data-lunch-result></strong><button class="button" type="submit">Save Lunch Inspection</button></div>` : ""}
          </form>` : ""}
          ${records.length ? `<details class="lunch-records" ${snapshot ? "" : "open"}><summary>Saved Lunch Inspections (${records.length})</summary><ul>${records.map(record => `<li><button type="button" data-lunch-record="${escape(record.id)}">Pipe # ${escape(record.pipe_number)} &middot; ${escape(record.result.toUpperCase())} &middot; ${escape(record.inspector_name)} &middot; ${escape(timestamp(record.submitted_at))}</button></li>`).join("")}</ul></details>` : ""}`;
        content.querySelectorAll("[data-lunch-record]").forEach(button => button.addEventListener("click", async () => {
          try {
            const data = await request(`${recordEndpoint}${encodeURIComponent(button.dataset.lunchRecord)}`, {}, "Loading lunch inspection...");
            current = data.record;
            render();
          } catch (error) { showError(error.message); }
        }));
        const form = content.querySelector("[data-lunch-form]");
        if (!form) return;
        if (saved) { form.addEventListener("submit", event => event.preventDefault()); return; }
        const evaluate = () => {
          let failed = false;
          let complete = true;
          form.querySelectorAll("[data-lunch-reading]").forEach(input => {
            const element = snapshot.elements.find(row => Number(row.element_sequence) === Number(input.dataset.sequence));
            const raw = input.value.trim();
            input.classList.remove("measurement-pass", "measurement-fail");
            if (!raw) { complete = false; return; }
            let pass;
            if (element.capture_type === "boolean") pass = raw === "Yes";
            else {
              const value = window.resolveInspectionMeasurement(raw, element.nominal).value;
              pass = Number.isFinite(value) && (element.min === null || element.min === undefined || value >= Number(Number(element.min).toPrecision(15))) && (element.max === null || element.max === undefined || value <= Number(Number(element.max).toPrecision(15)));
            }
            input.classList.add(pass ? "measurement-pass" : "measurement-fail");
            failed ||= !pass;
          });
          const result = form.querySelector("[data-lunch-result]");
          result.textContent = complete ? failed ? "Fail" : "Pass" : "";
          result.className = failed ? "lunch-fail" : "lunch-pass";
        };
        form.addEventListener("input", evaluate);
        form.addEventListener("change", evaluate);
        form.addEventListener("submit", async event => {
          event.preventDefault();
          if (busy) return;
          const button = form.querySelector('button[type="submit"]');
          button.disabled = true;
          try {
            const values = Object.fromEntries(Array.from(form.querySelectorAll("[data-lunch-reading]")).map(input => [input.dataset.sequence, input.value]));
            const data = await request(`/workflow/lunch/${encodeURIComponent(current.id)}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ csrf, adp: form.elements.adp.value, values }) }, "Saving lunch inspection...");
            current = data.record;
            records = [{ id: current.id, pipe_number: current.snapshot.pipe_number, result: current.result, inspector_name: current.inspector_name, submitted_at: current.submitted_at }, ...records.filter(record => record.id !== current.id)];
            render();
          } catch (error) { showError(error.message); button.disabled = false; }
        });
      };
      dialog.querySelector("[data-lunch-close]").addEventListener("click", () => { if (!busy) dialog.close(); });
      dialog.addEventListener("cancel", event => { if (busy) event.preventDefault(); });
      dialog.closest("section").addEventListener("click", async event => {
        const button = event.target.closest("[data-lunch-view]");
        if (!button || busy) return;
        content.replaceChildren();
        dialog.showModal();
        try {
          const data = await request(`${recordEndpoint}${encodeURIComponent(button.dataset.lunchView)}`, {}, "Loading lunch inspection...");
          current = data.record;
          records = [];
          render();
        } catch (error) { showError(error.message); }
      });
      openButton?.addEventListener("click", async () => {
        if (busy) return;
        content.replaceChildren();
        dialog.showModal();
        try {
          const data = await request("/workflow/lunch/open", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ csrf: dialog.dataset.csrf }) }, "Loading lunch comparison...");
          current = data.draft;
          csrf = data.csrf;
          records = data.records;
          render();
          if (data.message) showError(data.message);
          content.querySelector('[name="adp"]')?.focus();
        } catch (error) { showError(error.message); }
      });
    });
  };
  document.addEventListener("DOMContentLoaded", () => window.initializeLunchInspection(document));
})();
