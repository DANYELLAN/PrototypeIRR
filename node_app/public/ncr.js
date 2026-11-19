(() => {
  let catalogPromise;
  const catalog = () => catalogPromise ||= fetch("/public/ncr-options.json").then((response) => {
    if (!response.ok) throw new Error("NCR choices could not be loaded. Reload the page.");
    return response.json();
  });
  window.initializeNcrCapture = (root = document) => {
    root.querySelectorAll("[data-ncr-fields]").forEach((fields) => {
      if (fields.dataset.initialized) return;
      fields.dataset.initialized = "true";
      const ncrType = fields.querySelector('[name="ncr_ncr_type"]');
      const supplier = fields.querySelector('[name="ncr_supplier"]');
      const updateSupplier = () => { if (supplier) supplier.required = ncrType.value === "Supplier"; };
      ncrType.addEventListener("change", updateSupplier);
      updateSupplier();
      const engineering = fields.querySelector("[data-ncr-engineering]");
      engineering?.addEventListener("change", () => {
        fields.querySelector('[name="ncr_engineering_report"]').value = engineering.checked ? "true" : "false";
      });
      const tier1 = fields.querySelector('[name="ncr_tier1_id"]');
      const tier2 = fields.querySelector('[name="ncr_tier2_id"]');
      const updateTier2 = async (reset) => {
        try {
          const data = await catalog();
          const previous = reset ? "" : tier2.value;
          const choices = data.tier2.filter((row) => String(row.tier1_id) === tier1.value);
          tier2.replaceChildren(new Option(choices.length ? "" : "No Tier 2 codes", ""), ...choices.map((row) => new Option(row.label, row.id)));
          tier2.value = previous;
          tier2.required = choices.length > 0;
          tier2.disabled = choices.length === 0;
        } catch (error) {
          tier2.setCustomValidity(error.message);
        }
      };
      tier1.addEventListener("change", () => { tier2.setCustomValidity(""); updateTier2(true); });
      updateTier2(false);
      const files = fields.querySelector("[data-ncr-files]");
      if (!files) return;
      files.addEventListener("change", async () => {
        const errorLabel = fields.querySelector("[data-ncr-file-error]");
        files.setCustomValidity("");
        errorLabel.textContent = "";
        try {
          if (files.files.length > 5 || [...files.files].reduce((sum, file) => sum + file.size, 0) > 4 * 1024 * 1024) {
            throw new Error("Choose up to five files totaling no more than 4 MB.");
          }
          files.setCustomValidity("Please wait for attachments to finish loading.");
          const attachments = await Promise.all([...files.files].map((file) => new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve({name: file.name, content: String(reader.result).split(",")[1]});
            reader.onerror = () => reject(new Error("Unable to read attachment."));
            reader.readAsDataURL(file);
          })));
          fields.querySelector('[name="ncr_attachments"]').value = JSON.stringify(attachments);
          fields.querySelector("[data-ncr-file-names]").textContent = attachments.map((file) => file.name).join(", ");
          files.setCustomValidity("");
        } catch (error) {
          errorLabel.textContent = error.message;
          files.setCustomValidity(error.message);
        }
      });
    });
  };
  document.addEventListener("DOMContentLoaded", () => window.initializeNcrCapture(document));
  document.addEventListener("input", (event) => {
    if (!event.target.closest("[data-ncr-review-form]")) return;
    const send = document.querySelector("[data-ncr-send]");
    if (send) send.disabled = true;
    const message = document.querySelector("[data-ncr-unsaved]");
    if (message) message.textContent = "Save and verify your changes before sending.";
  });
})();
