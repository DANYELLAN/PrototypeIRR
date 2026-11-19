export function editMeasurement(value, start, end, key) {
  const text = String(value);
  start = Math.max(0, Math.min(start ?? text.length, text.length));
  end = Math.max(start, Math.min(end ?? start, text.length));
  if (key === "clear") return { value: "", caret: 0 };
  if (key === "+" || key === "-") {
    const oldSign = /^[+-]/.test(text) ? text[0] : "";
    const newSign = oldSign === key ? "" : key;
    return { value: newSign + text.slice(oldSign.length), caret: Math.max(newSign.length, start + newSign.length - oldSign.length) };
  }
  if (key === "backspace" && start === end) start = Math.max(0, start - 1);
  const inserted = key === "backspace" ? "" : key;
  const next = text.slice(0, start) + inserted + text.slice(end);
  if (!/^[+-]?\d*(?:\.\d*)?$/.test(next) || next.length > 64) return { value: text, caret: end };
  return { value: next, caret: start + inserted.length };
}

export function editPipeNumber(value, start, end, key) {
  const text = String(value);
  start = Math.max(0, Math.min(start ?? text.length, text.length));
  end = Math.max(start, Math.min(end ?? start, text.length));
  if (key === "clear") return { value: "", caret: 0 };
  if (key === "backspace" && start === end) start = Math.max(0, start - 1);
  const inserted = key === "backspace" ? "" : key;
  if (inserted && !/^[0-9.+-]$/.test(inserted)) return { value: text, caret: end };
  return { value: text.slice(0, start) + inserted + text.slice(end), caret: start + inserted.length };
}
