(() => {
  const key = "cnc-click-diagnostics-v1";
  const uid = () => globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  let queue = [];
  try { queue = JSON.parse(sessionStorage.getItem(key) || "[]"); } catch {}
  if (!Array.isArray(queue)) queue = [];
  let sending = false;
  let interaction = "";
  const browser = (() => {
    try {
      let value = sessionStorage.getItem(`${key}-browser`);
      if (!value) sessionStorage.setItem(`${key}-browser`, value = uid());
      return value;
    } catch { return uid(); }
  })();
  const save = () => { try { sessionStorage.setItem(key, JSON.stringify(queue.slice(-200))); } catch {} };
  async function flush() {
    if (sending || !queue.length) return;
    sending = true;
    const batch = queue.slice(0, 50);
    try {
      const response = await fetch("/diagnostics/events", { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ events: batch }), keepalive: true });
      if (response.ok) {
        const sent = new Set(batch.map((event) => event.id));
        queue = queue.filter((event) => !sent.has(event.id));
        save();
      }
    } catch {} finally { sending = false; }
  }
  function controlName(element) {
    if (!(element instanceof Element)) return "unknown";
    const button = element.closest('button, a, input[type="submit"], input[type="button"], [role="button"]');
    // Only button/link labels; never collect entered values or field contents.
    if (button) return `${button.tagName.toLowerCase()}#${button.id || ""} ${button instanceof HTMLInputElement ? button.name || button.type : button.textContent.trim().replace(/\s+/g, " ").slice(0, 90)}`;
    return `${element.tagName.toLowerCase()}#${element.id || ""}`;
  }
  function emit(kind, target, extra = {}) {
    queue.push({ id: uid(), kind, client_at: new Date().toISOString(), page: location.pathname,
      control: controlName(target), interaction, browser, disabled: Boolean(target?.closest?.(":disabled, [aria-disabled='true']")), ...extra });
    queue = queue.slice(-200);
    save();
    void flush();
  }
  document.addEventListener("pointerdown", (event) => {
    interaction = uid();
    emit("pointerdown", event.target);
  }, true);
  document.addEventListener("click", (event) => {
    if (event.detail === 0 || !interaction) interaction = uid();
    const form = event.target.closest?.("button, input")?.form;
    emit("click", event.target, { action: form ? new URL(form.action).pathname : "" });
  }, true);
  document.addEventListener("invalid", (event) => emit("invalid", event.target, { action: new URL(event.target.form.action).pathname }), true);
  document.addEventListener("submit", (event) => {
    if (!interaction) interaction = uid();
    const form = event.target;
    let field = form.querySelector('input[name="_click_interaction"]');
    if (!field) {
      field = document.createElement("input");
      field.type = "hidden";
      field.name = "_click_interaction";
      form.appendChild(field);
    }
    field.value = interaction;
    emit("submit", event.submitter || form, { action: new URL(form.action).pathname });
    queueMicrotask(() => {
      if (event.defaultPrevented) emit("submit_cancelled", event.submitter || form, { action: new URL(form.action).pathname });
    });
  }, true);
  window.addEventListener("error", () => emit("script_error", null));
  window.addEventListener("unhandledrejection", () => emit("script_error", null));
  window.addEventListener("pageshow", () => emit("page_loaded", document.body));
  window.addEventListener("pagehide", () => { save(); void flush(); });
  window.addEventListener("online", flush);
  window.setInterval(flush, 5000);
})();
