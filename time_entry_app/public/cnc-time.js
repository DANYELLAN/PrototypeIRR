window.addEventListener("load", () => {
  const dailyReview = document.getElementById("daily-time-review");
  if (dailyReview) {
    dailyReview.showModal();
    dailyReview.addEventListener("cancel", (event) => event.preventDefault());
    document.getElementById("daily-review-correct").addEventListener("click", () => {
      document.getElementById("daily-review-confirm").hidden = false;
      document.getElementById("daily-review-reason").focus();
    });
    document.getElementById("daily-review-confirm").addEventListener("submit", (event) => {
      const reason = document.getElementById("daily-review-reason");
      reason.setCustomValidity(reason.value.trim() ? "" : "A reason is required.");
      if (!reason.reportValidity()) event.preventDefault();
    });
    document.getElementById("daily-review-reason").addEventListener("input", (event) => event.target.setCustomValidity(""));
  }
  const headerClock = document.getElementById("header_clock");
  if (headerClock) {
    const updateHeaderClock = () => {
      headerClock.textContent = new Date().toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      });
    };
    updateHeaderClock();
    window.setInterval(updateHeaderClock, 1000);
  }

  const adpInput = document.getElementById("adp_number_input");
  const employeeMatch = document.getElementById("employee_match");

  if (adpInput && employeeMatch) {
    let lookupTimer = null;
    let activeLookup = null;

    const resetMatchState = () => {
      employeeMatch.textContent = "Employee match will appear here.";
      employeeMatch.classList.remove("success", "warning");
    };

    const setMatchState = (message, kind) => {
      employeeMatch.textContent = message;
      employeeMatch.classList.toggle("success", kind === "success");
      employeeMatch.classList.toggle("warning", kind === "warning");
    };

    const matchMessage = (value) => {
      const trimmed = String(value ?? "").trim();
      if (!trimmed) {
        if (activeLookup && activeLookup.readyState !== 4) {
          activeLookup.abort();
        }
        resetMatchState();
        return;
      }

      if (activeLookup && activeLookup.readyState !== 4) {
        activeLookup.abort();
      }

      const xhr = new XMLHttpRequest();
      activeLookup = xhr;
      xhr.open("GET", `/lookup-employee?adp_number=${encodeURIComponent(trimmed)}`);
      xhr.onload = () => {
        if (xhr !== activeLookup) return;
        try {
          const response = JSON.parse(xhr.responseText || "{}");
          if (response.employee) {
            setMatchState(`Employee: ${response.employee.full_name} (${response.employee.emp_id})`, "success");
          } else {
            setMatchState("No active Ennis employee found for that ADP number.", "warning");
          }
        } catch {
          setMatchState("No active Ennis employee found for that ADP number.", "warning");
        }
      };
      xhr.onerror = () => {
        if (xhr !== activeLookup) return;
        setMatchState("No active Ennis employee found for that ADP number.", "warning");
      };
      xhr.abort = xhr.abort || (() => {});
      xhr.send();
    };

    const scheduleLookup = (value) => {
      clearTimeout(lookupTimer);
      const trimmed = String(value ?? "").trim();
      if (!trimmed) {
        resetMatchState();
        return;
      }
      lookupTimer = window.setTimeout(() => matchMessage(trimmed), 160);
    };

    adpInput.addEventListener("input", (event) => scheduleLookup(event.target.value));
    adpInput.addEventListener("change", (event) => scheduleLookup(event.target.value));
    adpInput.addEventListener("blur", (event) => {
      clearTimeout(lookupTimer);
      matchMessage(event.target.value);
    });
  }

  if ("serviceWorker" in navigator) {
    navigator.serviceWorker.register("/cnc-time-sw.js", {scope: "/"}).catch(() => {});
  }

  const locationSelects = Array.from(document.querySelectorAll(".location-select"));
  const assetSelects = Array.from(document.querySelectorAll(".asset-select"));
  locationSelects.forEach((locationSelect) => {
    locationSelect.addEventListener("change", () => {
      const currentValue = locationSelect.value;
      assetSelects.forEach((assetSelect) => {
        Array.from(assetSelect.options).forEach((option) => {
          const optionLocation = option.dataset.location;
          option.hidden = Boolean(currentValue) && Boolean(optionLocation) && optionLocation !== currentValue;
        });
      });
    });
    locationSelect.dispatchEvent(new Event("change"));
  });

  const operationSelects = Array.from(document.querySelectorAll(".operation-select"));
  operationSelects.forEach((operationSelect) => {
    const form = operationSelect.closest("form");
    const workOrderSelect = form ? form.querySelector('select[name="production_number"]') : null;
    if (!workOrderSelect) return;

    const syncOperations = () => {
      const workOrder = workOrderSelect.value;
      let visibleSelection = false;
      Array.from(operationSelect.options).forEach((option) => {
        const matches = !workOrder || option.dataset.wo === workOrder;
        option.hidden = !matches;
        if (matches && option.selected) visibleSelection = true;
      });
      if (!visibleSelection) {
        const firstVisible = Array.from(operationSelect.options).find((option) => !option.hidden);
        if (firstVisible) firstVisible.selected = true;
      }
    };

    workOrderSelect.addEventListener("change", syncOperations);
    syncOperations();
  });

  const liveMachineDashboard = document.querySelector("[data-live-machine-dashboard]");
  if (liveMachineDashboard) {
    const summaryTarget = document.getElementById("live_machine_summary");
    const graphTarget = document.getElementById("live_machine_graph");
    const errorTarget = document.getElementById("live_machine_error");
    const pollMs = Math.max(Number(liveMachineDashboard.dataset.pollMs || 30000), 5000);
    let activeRefresh = false;

    const setLiveError = (message) => {
      if (!errorTarget) return;
      errorTarget.innerHTML = message
        ? `<div class="cnc-notice warning">${message.replace(/[&<>"']/g, (character) => ({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#39;",
          })[character])}</div>`
        : "";
    };

    const refreshLiveMachine = async () => {
      if (activeRefresh) return;
      activeRefresh = true;
      try {
        const response = await fetch("/dashboard/live-machine.json", {
          headers: { Accept: "application/json" },
          cache: "no-store",
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || payload.ok === false) {
          throw new Error(payload.message || "LiveView data is unavailable.");
        }
        if (summaryTarget && payload.summary_html) summaryTarget.innerHTML = payload.summary_html;
        if (graphTarget && payload.graph_html) graphTarget.innerHTML = payload.graph_html;
        setLiveError("");
      } catch (error) {
        setLiveError(error.message || "LiveView data is unavailable.");
      } finally {
        activeRefresh = false;
      }
    };

    window.setInterval(refreshLiveMachine, pollMs);
  }

  const notificationWatch = document.querySelector("[data-notification-watch]");
  const openNotificationStates = new Set(["pending", "assistance_requested"]);
  if (notificationWatch && openNotificationStates.has(notificationWatch.dataset.notificationState)) {
    const requestId = notificationWatch.dataset.notificationWatch;
    const statusTargets = document.querySelectorAll(`[data-notification-status="${requestId}"]`);
    const responseTarget = notificationWatch.querySelector("[data-notification-response]");
    const statusClasses = {
      approved_to_run: "",
      do_not_run: "stopped",
      pending: "inactive",
      assistance_requested: "warning",
      resolved: "",
    };
    let notificationRefreshPending = false;
    const refreshNotification = async () => {
      if (notificationRefreshPending) return;
      notificationRefreshPending = true;
      try {
        const response = await fetch(`/notifications/status.json?id=${encodeURIComponent(requestId)}`, {
          headers: { Accept: "application/json" },
          cache: "no-store",
        });
        if (!response.ok) return;
        const payload = await response.json();
        statusTargets.forEach((target) => {
          target.textContent = payload.status_label;
          target.className = `status-badge ${statusClasses[payload.status] ?? "inactive"}`;
        });
        if (responseTarget) {
          responseTarget.textContent = payload.refresh_error || (payload.responder_name
            ? `${payload.status === "resolved" ? "Resolved" : "Response"} by ${payload.responder_name}${payload.response_note ? `: ${payload.response_note}` : ""}${payload.responded_at ? ` (${new Date(payload.responded_at).toLocaleString()})` : ""}`
            : "");
        }
        notificationWatch.dataset.notificationState = payload.status;
        if (!openNotificationStates.has(payload.status)) window.clearInterval(notificationWatch.pollTimer);
      } catch {
        // Keep the current state visible and try again on the next interval.
      } finally {
        notificationRefreshPending = false;
      }
    };
    notificationWatch.pollTimer = window.setInterval(refreshNotification, 10000);
  }
});

import("/public/tablet-session-lock.js").catch(()=>{});

// Ask for relief identity in a touch-friendly modal before submitting the original form.
let reliefDialog, pendingReliefForm, pendingReliefSubmitter;
const confirmedReliefForms=new WeakSet();
function openReliefDialog(form, submitter){
 if(!reliefDialog){
  reliefDialog=document.createElement('dialog');
  reliefDialog.id='relief-adp-dialog';reliefDialog.className='cnc-relief-dialog';
  reliefDialog.setAttribute('aria-labelledby','relief-adp-heading');
  reliefDialog.setAttribute('aria-describedby','relief-adp-description');
  reliefDialog.innerHTML='<div class="relief-dialog-header"><p class="relief-eyebrow">Break coverage</p><h2 id="relief-adp-heading">Start break with relief</h2><p id="relief-adp-description">Enter the ADP number of the operator taking over.</p></div><form id="relief-adp-form" class="cnc-form relief-dialog-body"><div class="relief-production-note"><strong>Production stays running</strong><span>This break will not count as downtime.</span></div><label for="relief-adp-input">Relief operator ADP number</label><input id="relief-adp-input" name="relief_adp" inputmode="numeric" pattern="[0-9]+" required autocomplete="off" placeholder="Enter ADP number" aria-describedby="relief-adp-note relief-adp-error" autofocus><p id="relief-adp-note">Use the ADP number of the person providing relief.</p><p id="relief-adp-error" role="alert"></p><div class="relief-dialog-actions"><button class="cnc-button ghost" id="relief-adp-cancel" type="button">Cancel</button><button class="cnc-button" type="submit">Confirm Relief</button></div></form>';
  document.body.append(reliefDialog);
  reliefDialog.querySelector('#relief-adp-cancel').addEventListener('click',()=>reliefDialog.close());
  reliefDialog.addEventListener('close',()=>{pendingReliefForm=null;pendingReliefSubmitter=null;});
  reliefDialog.querySelector('form').addEventListener('submit',event=>{
   event.preventDefault();if(!pendingReliefForm)return;
   const adp=reliefDialog.querySelector('[name="relief_adp"]').value.trim();
   if(!/^\d+$/.test(adp)){reliefDialog.querySelector('#relief-adp-error').textContent='Enter a valid ADP number.';return;}
   const original=pendingReliefForm,button=pendingReliefSubmitter;
   let field=original.querySelector('[name="relief_adp"]');
   if(!field){field=document.createElement('input');field.type='hidden';field.name='relief_adp';original.append(field);}
   field.value=adp;confirmedReliefForms.add(original);reliefDialog.close();
   original.requestSubmit(button||undefined);
  });
 }
 if(reliefDialog.open)return;
 pendingReliefForm=form;pendingReliefSubmitter=submitter;
 reliefDialog.querySelector('form').reset();
 reliefDialog.querySelector('#relief-adp-error').textContent='';
 reliefDialog.querySelector('#relief-adp-heading').textContent=form.getAttribute('action')==='/time/relief-arrived'?'Relief has arrived':'Start break with relief';
 reliefDialog.showModal();reliefDialog.querySelector('[name="relief_adp"]').focus();
}
document.addEventListener('submit',event=>{
 const form=event.target;
 if(!form.matches?.('form[action="/time/relief-arrived"], form[action="/time/pause"]'))return;
 if(form.getAttribute('action')==='/time/pause'&&form.elements.break_type?.value!=='With Relief')return;
 if(confirmedReliefForms.delete(form))return;
 event.preventDefault();openReliefDialog(form,event.submitter);
});

if(document.querySelector('form[action="/time/stop"],form[data-setup-handoff]')){
 import('/public/lunch-reminder.js').then(({installLunchReminder,lunchForm})=>installLunchReminder({getActive:()=>{
  const form=lunchForm();if(!form)return null;
  const token=form.elements.entry_id?.value;if(!token)return null;
  const owner=document.getElementById('earlier-tablet-work')?.dataset.owner||token;
  let shift='40';try{shift=JSON.parse(owner)[2]||shift;}catch{}
  const missedLunch=document.querySelector('form[action="/time/active/log-lunch"]');
  return {form,token,owner,shift,logLunch:missedLunch?day=>{
   missedLunch.elements.lunch_day.value=day;missedLunch.requestSubmit();return new Promise(()=>{});
  }:undefined};
 }})).catch(()=>{});
}
