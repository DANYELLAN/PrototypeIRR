(() => {
  const dialog = document.getElementById("inspection-loading");
  if (!dialog) return;
  const label = dialog.querySelector("[data-loading-label]");
  const pending = new Map();
  let nextId = 0;
  const refresh = () => {
    document.querySelector("main")?.setAttribute("aria-busy", String(pending.size > 0));
    if (pending.size) {
      label.textContent = Array.from(pending.values()).at(-1);
      if (!dialog.open) dialog.showModal();
    } else if (dialog.open) dialog.close();
  };
  const start = (message = "Loading...") => {
    const id = ++nextId;
    pending.set(id, message);
    refresh();
    return () => { pending.delete(id); refresh(); };
  };
  const reset = () => { pending.clear(); refresh(); };
  dialog.addEventListener("cancel", event => event.preventDefault());
  dialog.close();
  window.inspectionLoading = { start };

  if (document.readyState !== "complete") {
    const finish = start();
    window.addEventListener("load", finish, { once: true });
  }
  window.addEventListener("pageshow", reset);
  window.addEventListener("beforeunload", () => start());
  document.addEventListener("submit", event => {
    queueMicrotask(() => {
      if (!event.defaultPrevented && event.target.method !== "dialog" && event.target.target !== "_blank") start();
    });
  });
  document.addEventListener("click", event => {
    const link = event.target.closest("a[href]");
    if (!link || event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey || link.hasAttribute("download") || (link.target && link.target !== "_self")) return;
    const url = new URL(link.href, location.href);
    if (url.origin !== location.origin || !["http:", "https:"].includes(url.protocol)) return;
    if (url.pathname === location.pathname && url.search === location.search && url.hash) return;
    queueMicrotask(() => { if (!event.defaultPrevented) start(); });
  });
})();
