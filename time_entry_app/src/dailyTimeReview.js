function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (character) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[character]);
}

export function dailyTimeReviewMarkup(review, { editing = false, error = "" } = {}) {
  if (!review?.required) return "";
  if (editing) {
    return `<div class="cnc-notice warning daily-review-banner">
      <span>Daily working time: ${escapeHtml(review.total)}. Review your entries.</span>
      <form method="post" action="/time/review/show"><button class="cnc-button" type="submit">Review Time</button></form>
    </div>`;
  }
  return `<dialog id="daily-time-review" class="daily-time-review" aria-labelledby="daily-review-title" aria-describedby="daily-review-message">
    <h2 id="daily-review-title">Review Your Time</h2>
    <p id="daily-review-message">Your working time for the day exceeds 10.5 hours. Please verify your time entries.</p>
    ${error ? `<div class="cnc-notice warning" role="alert">${escapeHtml(error)}</div>` : ""}
    <p><strong>${escapeHtml(review.labor_date)} &middot; ${escapeHtml(review.total)} total &middot; 10:30 limit</strong></p>
    <div class="cnc-table-wrap daily-review-entries"><table class="cnc-table">
      <thead><tr><th>Shift</th><th>Work Order</th><th>Operation</th><th>Detail</th><th>Status</th><th>Time</th></tr></thead>
      <tbody>${review.entries.map((row) => `<tr>
        <td>${escapeHtml(row.shift)}</td><td>${escapeHtml(row.production_number)}</td>
        <td>${escapeHtml(row.operation_id)}</td><td>${escapeHtml(row.details_type_ii || row.details_type)}</td>
        <td>${escapeHtml(row.status)}</td><td>${escapeHtml(row.total)}</td>
      </tr>`).join("")}</tbody>
    </table></div>
    <div class="daily-review-actions">
      <form method="post" action="/time/review/edit"><button class="cnc-button ghost" type="submit" autofocus>Go Back and Edit Entries</button></form>
      <button class="cnc-button" id="daily-review-correct" type="button">Entries Are Correct</button>
    </div>
    <form method="post" action="/time/review/confirm" id="daily-review-confirm" class="cnc-form" hidden>
      <input type="hidden" name="labor_date" value="${escapeHtml(review.labor_date)}" />
      <input type="hidden" name="snapshot_hash" value="${escapeHtml(review.snapshot_hash)}" />
      <label><span>Reason for Exceeding 10.5 Hours</span><textarea name="reason" id="daily-review-reason" required maxlength="2000" rows="3"></textarea></label>
      <button class="cnc-button" type="submit">Confirm Entries</button>
    </form>
  </dialog>`;
}
