var A=`
  :host {
    display: block;
    box-sizing: border-box;
    font-family: var(--ha-card-font-family, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif);
    color: var(--primary-text-color, #212121);
    --ev-primary: var(--primary-color, #03a9f4);
    --ev-card-bg: var(--ha-card-background, var(--card-background-color, #ffffff));
    --ev-text-primary: var(--primary-text-color, #212121);
    --ev-text-secondary: var(--secondary-text-color, #727272);
    --ev-border: var(--ha-card-border-color, var(--divider-color, #e0e0e0));
    --ev-success: var(--success-color, #4caf50);
    --ev-warning: var(--warning-color, #ff9800);
    --ev-error: var(--error-color, #f44336);
    --ev-info: var(--info-color, #2196f3);
    --ev-radius: var(--ha-card-border-radius, 12px);
  }

  ha-card, .ev-card-container {
    background: var(--ev-card-bg);
    border-radius: var(--ev-radius);
    border: var(--ha-card-border-width, 1px) solid var(--ev-border);
    overflow: hidden;
    display: flex;
    flex-direction: column;
    position: relative;
    box-shadow: var(--ha-card-box-shadow, 0 2px 4px rgba(0,0,0,0.06));
    max-width: 100%;
    margin: 0;
  }

  /* Header */
  .ev-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 14px 16px;
    border-bottom: 1px solid var(--ev-border);
    background: var(--ev-card-bg);
    gap: 8px;
    flex-wrap: wrap;
  }

  .ev-header-title-wrap {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .ev-title {
    font-size: 1.15rem;
    font-weight: 600;
    margin: 0;
    color: var(--ev-text-primary);
  }

  .ev-header-actions {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .ev-icon-button {
    background: transparent;
    border: 1px solid var(--ev-border);
    border-radius: 6px;
    color: var(--ev-text-primary);
    cursor: pointer;
    padding: 6px 10px;
    font-size: 0.85rem;
    display: inline-flex;
    align-items: center;
    gap: 4px;
    transition: background-color 0.15s ease, border-color 0.15s ease;
  }

  .ev-icon-button:hover:not(:disabled) {
    background-color: rgba(128, 128, 128, 0.1);
  }

  .ev-icon-button:focus-visible {
    outline: 2px solid var(--ev-primary);
    outline-offset: 1px;
  }

  .ev-icon-button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  /* Status message banner */
  .ev-banner {
    padding: 8px 16px;
    font-size: 0.85rem;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .ev-banner-error {
    background: rgba(244, 67, 54, 0.12);
    color: var(--ev-error);
    border-bottom: 1px solid rgba(244, 67, 54, 0.3);
  }
  .ev-banner-info {
    background: rgba(3, 169, 244, 0.12);
    color: var(--ev-primary);
    border-bottom: 1px solid rgba(3, 169, 244, 0.3);
  }

  /* Feed list */
  .ev-feed {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    max-height: 600px;
    overflow-y: auto;
    overflow-x: hidden;
  }

  .ev-events-list {
    display: flex;
    flex-direction: column;
    width: 100%;
  }

  .ev-empty-box, .ev-loading-box {
    padding: 32px 16px;
    text-align: center;
    color: var(--ev-text-secondary);
    font-size: 0.95rem;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 8px;
  }

  .ev-row {
    display: flex;
    flex-direction: row;
    align-items: center;
    padding: 12px 16px;
    border-bottom: 1px solid var(--ev-border);
    cursor: pointer;
    background: var(--ev-card-bg);
    transition: background-color 0.15s ease;
    gap: 12px;
    text-align: left;
    outline: none;
    box-sizing: border-box;
    width: 100%;
  }

  .ev-row:last-child {
    border-bottom: none;
  }

  .ev-row:hover, .ev-row:focus-visible {
    background: rgba(128, 128, 128, 0.08);
  }

  .ev-row:focus-visible {
    outline: 2px solid var(--ev-primary);
    outline-offset: -2px;
  }

  /* Time column */
  .ev-col-time {
    flex: 0 0 74px;
    display: flex;
    align-items: center;
  }

  .ev-time-text {
    font-size: 0.82rem;
    font-weight: 500;
    color: var(--ev-text-secondary);
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }

  /* Info column */
  .ev-col-info {
    flex: 1 1 auto;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .ev-event-title {
    font-weight: 600;
    font-size: 0.95rem;
    color: var(--ev-text-primary);
    margin: 0;
    word-break: break-word;
    line-height: 1.35;
  }

  .ev-event-meta {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
    font-size: 0.8rem;
    line-height: 1.4;
  }

  .ev-source-badge {
    display: inline-flex;
    align-items: center;
    font-weight: 600;
    font-size: 0.74rem;
    color: var(--ev-text-primary);
    background: rgba(128, 128, 128, 0.14);
    border: 1px solid var(--ev-border);
    padding: 2px 7px;
    border-radius: 4px;
    white-space: nowrap;
  }

  .ev-kind-badge {
    display: inline-flex;
    align-items: center;
    font-weight: 500;
    font-size: 0.74rem;
    color: var(--ev-text-secondary);
    background: rgba(128, 128, 128, 0.08);
    border: 1px solid var(--ev-border);
    padding: 2px 6px;
    border-radius: 4px;
    white-space: nowrap;
  }

  .ev-triggers-text {
    font-size: 0.78rem;
    color: var(--ev-text-secondary);
    word-break: break-word;
  }

  /* Badges column */
  .ev-col-badges {
    flex: 0 0 auto;
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 6px;
  }

  .ev-status-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
    font-size: 0.72rem;
    font-weight: 600;
    padding: 3px 8px;
    border-radius: 12px;
    white-space: nowrap;
    line-height: 1.25;
    letter-spacing: 0.01em;
    text-transform: none;
  }

  .badge-success {
    background: rgba(76, 175, 80, 0.16);
    color: var(--ev-success, #4caf50);
    border: 1px solid rgba(76, 175, 80, 0.35);
  }

  .badge-recording {
    background: rgba(244, 67, 54, 0.16);
    color: var(--ev-error, #f44336);
    border: 1px solid rgba(244, 67, 54, 0.35);
    animation: ev-pulse 1.8s infinite;
  }

  .badge-running {
    background: rgba(33, 150, 243, 0.16);
    color: var(--ev-info, #2196f3);
    border: 1px solid rgba(33, 150, 243, 0.35);
    animation: ev-pulse 1.8s infinite;
  }

  .badge-warning {
    background: rgba(255, 152, 0, 0.16);
    color: var(--ev-warning, #ff9800);
    border: 1px solid rgba(255, 152, 0, 0.35);
  }

  .badge-error {
    background: rgba(244, 67, 54, 0.16);
    color: var(--ev-error, #f44336);
    border: 1px solid rgba(244, 67, 54, 0.35);
  }

  .badge-info {
    background: rgba(33, 150, 243, 0.16);
    color: var(--ev-info, #2196f3);
    border: 1px solid rgba(33, 150, 243, 0.35);
  }

  .badge-muted {
    background: rgba(128, 128, 128, 0.14);
    color: var(--ev-text-secondary, #888888);
    border: 1px solid rgba(128, 128, 128, 0.3);
  }

  @keyframes ev-pulse {
    0% { opacity: 1; }
    50% { opacity: 0.55; }
    100% { opacity: 1; }
  }

  /* Load more pagination */
  .ev-footer {
    padding: 10px 16px;
    border-top: 1px solid var(--ev-border);
    display: flex;
    justify-content: center;
    background: var(--ev-card-bg);
  }

  .ev-load-more-btn {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid var(--ev-border);
    border-radius: 6px;
    background: transparent;
    color: var(--ev-primary);
    font-weight: 600;
    cursor: pointer;
    font-size: 0.9rem;
  }

  .ev-load-more-btn:hover:not(:disabled) {
    background: rgba(128, 128, 128, 0.08);
  }

  /* Detail Drawer / Modal */
  .ev-drawer-backdrop {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.5);
    z-index: 10;
    display: flex;
    justify-content: flex-end;
  }

  .ev-drawer {
    width: 100%;
    max-width: 480px;
    height: 100%;
    background: var(--ev-card-bg);
    display: flex;
    flex-direction: column;
    overflow-y: auto;
    overflow-x: hidden;
    z-index: 11;
    box-shadow: -4px 0 16px rgba(0,0,0,0.25);
    animation: ev-slide-in 0.2s cubic-bezier(0, 0, 0.2, 1);
  }

  @keyframes ev-slide-in {
    from { transform: translateX(100%); }
    to { transform: translateX(0); }
  }

  .ev-drawer-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 14px 16px;
    border-bottom: 1px solid var(--ev-border);
    gap: 8px;
    position: sticky;
    top: 0;
    background: var(--ev-card-bg);
    z-index: 2;
  }

  .ev-drawer-title {
    font-size: 1.05rem;
    font-weight: 600;
    margin: 0;
    color: var(--ev-text-primary);
    word-break: break-word;
  }

  .ev-drawer-content {
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 16px;
  }

  /* Media Player Area */
  .ev-media-section {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .ev-clip-selector {
    display: flex;
    gap: 8px;
    overflow-x: auto;
    padding-bottom: 4px;
  }

  .ev-clip-tab {
    padding: 4px 10px;
    font-size: 0.8rem;
    border-radius: 6px;
    border: 1px solid var(--ev-border);
    background: transparent;
    color: var(--ev-text-primary);
    cursor: pointer;
    white-space: nowrap;
  }

  .ev-clip-tab.active {
    background: var(--ev-primary);
    color: #ffffff;
    border-color: var(--ev-primary);
    font-weight: 600;
  }

  .ev-video-container {
    width: 100%;
    aspect-ratio: 16 / 9;
    background: #000000;
    border-radius: 8px;
    overflow: hidden;
    position: relative;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .ev-video-player {
    width: 100%;
    height: 100%;
    object-fit: contain;
  }

  .ev-image-preview {
    max-width: 100%;
    max-height: 100%;
    object-fit: contain;
  }

  .ev-media-placeholder {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 8px;
    color: #e0e0e0;
    text-align: center;
    padding: 16px;
    font-size: 0.88rem;
  }

  .ev-media-retry-btn {
    padding: 6px 12px;
    border: 1px solid rgba(255,255,255,0.4);
    background: rgba(255,255,255,0.15);
    color: #ffffff;
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.8rem;
  }
  .ev-media-retry-btn:hover {
    background: rgba(255,255,255,0.25);
  }

  /* Detail Sections */
  .ev-section-title {
    font-size: 0.88rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--ev-text-secondary);
    margin: 0 0 8px 0;
  }

  .ev-summary-text, .ev-activity-text, .ev-objects-text {
    font-size: 0.95rem;
    line-height: 1.45;
    color: var(--ev-text-primary);
    margin: 0;
    word-break: break-word;
    background: rgba(128, 128, 128, 0.05);
    padding: 10px 12px;
    border-radius: 6px;
  }

  /* Chronology */
  .ev-chronology {
    list-style: none;
    margin: 0;
    padding: 0 0 0 12px;
    position: relative;
    border-left: 2px solid var(--ev-border);
    display: flex;
    flex-direction: column;
    gap: 12px;
  }

  .ev-chronology-item {
    position: relative;
    padding-left: 12px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
  }

  .ev-chronology-item::before {
    content: '';
    position: absolute;
    left: -18px;
    top: 50%;
    transform: translateY(-50%);
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--ev-primary);
    border: 2px solid var(--ev-card-bg);
  }

  .ev-chronology-badge {
    font-weight: 600;
    font-size: 0.82rem;
    color: var(--ev-text-primary);
  }

  .ev-chronology-offset {
    font-size: 0.78rem;
    color: var(--ev-text-secondary);
    background: rgba(128, 128, 128, 0.12);
    padding: 2px 6px;
    border-radius: 4px;
    font-family: monospace;
  }

  /* Uncertainty box */
  .ev-uncertainty-box {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    background: rgba(255, 152, 0, 0.12);
    border: 1px solid rgba(255, 152, 0, 0.35);
    color: var(--ev-text-primary);
    padding: 10px 12px;
    border-radius: 6px;
    font-size: 0.85rem;
  }

  /* Capture Error Box */
  .ev-capture-error-box {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    background: rgba(244, 67, 54, 0.12);
    border: 1px solid rgba(244, 67, 54, 0.35);
    color: var(--ev-error, #f44336);
    padding: 10px 12px;
    border-radius: 6px;
    font-size: 0.85rem;
    line-height: 1.4;
    word-break: break-word;
  }

  .ev-error-box {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    background: rgba(244, 67, 54, 0.12);
    border: 1px solid rgba(244, 67, 54, 0.35);
    color: var(--ev-error);
    padding: 10px 12px;
    border-radius: 6px;
    font-size: 0.85rem;
  }

  /* Drawer Actions */
  .ev-drawer-actions {
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-top: 8px;
    padding-top: 12px;
    border-top: 1px solid var(--ev-border);
  }

  .ev-retry-btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
    padding: 10px 16px;
    background: var(--ev-primary);
    color: #ffffff;
    border: none;
    border-radius: 6px;
    font-weight: 600;
    font-size: 0.9rem;
    cursor: pointer;
    transition: opacity 0.15s ease;
  }

  .ev-retry-btn:hover:not(:disabled) {
    opacity: 0.9;
  }

  .ev-retry-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  /* Mobile narrow <= 480px adjustments */
  @media (max-width: 480px) {
    .ev-header {
      padding: 10px 12px;
    }
    .ev-title {
      font-size: 1.05rem;
    }
    .ev-row {
      flex-direction: column;
      align-items: flex-start;
      gap: 8px;
      padding: 10px 12px;
    }
    .ev-col-time {
      flex: none;
      width: 100%;
    }
    .ev-col-info {
      width: 100%;
    }
    .ev-col-badges {
      width: 100%;
      flex-direction: row;
      flex-wrap: wrap;
      align-items: center;
      justify-content: flex-start;
      gap: 6px;
    }
    .ev-drawer {
      max-width: 100%;
    }
    .ev-drawer-content {
      padding: 12px;
    }
  }
`,q=`
  :host {
    display: block;
    box-sizing: border-box;
    font-family: var(--ha-card-font-family, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif);
    color: var(--primary-text-color, #212121);
    padding: 16px;
  }

  .form-group {
    display: flex;
    flex-direction: column;
    margin-bottom: 16px;
    gap: 6px;
  }

  label {
    font-size: 0.9rem;
    font-weight: 600;
    color: var(--primary-text-color, #212121);
  }

  .helper-text {
    font-size: 0.8rem;
    color: var(--secondary-text-color, #727272);
  }

  input[type="text"], input[type="number"], select {
    padding: 8px 12px;
    border: 1px solid var(--divider-color, #ccc);
    border-radius: 6px;
    background: var(--card-background-color, #fff);
    color: var(--primary-text-color, #212121);
    font-size: 0.95rem;
    box-sizing: border-box;
    width: 100%;
  }

  input:focus, select:focus {
    outline: 2px solid var(--primary-color, #03a9f4);
    outline-offset: -1px;
    border-color: var(--primary-color, #03a9f4);
  }

  .error-text {
    color: var(--error-color, #f44336);
    font-size: 0.8rem;
    font-weight: 500;
  }
`;var E=class extends HTMLElement{constructor(){super(),this.attachShadow({mode:"open"}),this._config={},this._hass=null}setConfig(e){this._config={...e},this._render()}set hass(e){this._hass=e}connectedCallback(){this._render()}_valueChanged(e,t){if(!this._config)return;let n={...this._config};if(t===void 0||t==="")delete n[e];else if(e==="page_size"){let o=parseInt(t,10);n[e]=isNaN(o)?20:Math.max(1,Math.min(100,o))}else n[e]=t.trim();this._config=n;let r=new CustomEvent("config-changed",{detail:{config:this._config},bubbles:!0,composed:!0});this.dispatchEvent(r)}_render(){if(!this.shadowRoot)return;let e=this._config.page_size!==void 0?this._config.page_size:20,t=this._config.source_id||"",n=this._config.kind||"";this.shadowRoot.innerHTML=`
      <style>${q}</style>
      <div class="card-config">
        <div class="form-group">
          <label for="source_id">Source ID (Optional)</label>
          <input
            id="source_id"
            type="text"
            placeholder="e.g. front_entrance"
            value="${this._escape(t)}"
          />
          <div class="helper-text">Filter feed events to a specific source identifier or leave blank for all sources.</div>
        </div>

        <div class="form-group">
          <label for="kind">Event Kind (Optional)</label>
          <input
            id="kind"
            type="text"
            placeholder="e.g. doorbell, motion, contact"
            value="${this._escape(n)}"
          />
          <div class="helper-text">Filter feed events to a specific incident kind or leave blank for all kinds.</div>
        </div>

        <div class="form-group">
          <label for="page_size">Page Size (1 \u2013 100)</label>
          <input
            id="page_size"
            type="number"
            min="1"
            max="100"
            step="1"
            value="${e}"
          />
          <div class="helper-text">Number of events to load per page (defaults to 20).</div>
        </div>
      </div>
    `;let r=this.shadowRoot.querySelector("#source_id"),o=this.shadowRoot.querySelector("#kind"),c=this.shadowRoot.querySelector("#page_size");r&&r.addEventListener("change",a=>this._valueChanged("source_id",a.target.value)),o&&o.addEventListener("change",a=>this._valueChanged("kind",a.target.value)),c&&c.addEventListener("change",a=>this._valueChanged("page_size",a.target.value))}_escape(e){return e==null?"":String(e).replace(/"/g,"&quot;")}};customElements.get("event-recording-card-editor")||customElements.define("event-recording-card-editor",E);function y(i){return i==null?"":String(i).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#39;")}function C(i,e){return e&&typeof e=="string"&&e.trim()?e.trim():!i||typeof i!="string"?"Source":i.split(/[-_]/).filter(Boolean).map(t=>t.charAt(0).toUpperCase()+t.slice(1).toLowerCase()).join(" ")||"Source"}function w(i,e){return e&&typeof e=="string"&&e.trim()?e.trim():!i||typeof i!="string"?"Event":i.split(/[-_]/).filter(Boolean).map(t=>t.charAt(0).toUpperCase()+t.slice(1).toLowerCase()).join(" ")||"Event"}function S(i,e){if(!i)return"--:--:--";try{let t=new Date(i);if(isNaN(t.getTime()))return String(i);let n={hour:"2-digit",minute:"2-digit",second:"2-digit",hour12:!1};return e&&(n.timeZone=e),new Intl.DateTimeFormat(void 0,n).format(t)}catch{return String(i)}}function I(i,e){if(!i)return"";try{let t=new Date(i);if(isNaN(t.getTime()))return String(i);let n={year:"numeric",month:"short",day:"numeric",hour:"2-digit",minute:"2-digit",second:"2-digit",timeZoneName:"short"};return e&&(n.timeZone=e),new Intl.DateTimeFormat(void 0,n).format(t)}catch{return String(i)}}function F(i,e){if(!i||!e)return"+0s";let t=new Date(e).getTime(),n=new Date(i).getTime();if(isNaN(t)||isNaN(n))return"+0s";let r=Math.round((n-t)/1e3);return r>=0?`+${r}s`:`${r}s`}function M(i,e){return e&&typeof e=="string"&&e.trim()?e.trim():w(i)}function L(i){if(!i)return"Incident recorded";if(i.label&&typeof i.label=="string"&&i.label.trim())return i.label.trim();let e=i.annotations?.event_vision;return e&&e.status==="complete"&&e.title&&typeof e.title=="string"&&e.title.trim()?e.title.trim():i.kind&&typeof i.kind=="string"&&i.kind.trim()?`${w(i.kind)} recorded`:"Incident recorded"}function D(i){switch(i){case"ready":return{label:"Capture: Ready",class:"badge-success",icon:"check-circle"};case"recording":return{label:"Capture: Recording",class:"badge-recording",icon:"record-rec"};case"requested":return{label:"Capture: Requested",class:"badge-info",icon:"clock-outline"};case"partial":return{label:"Capture: Partial",class:"badge-warning",icon:"alert-circle-outline"};case"interrupted":return{label:"Capture: Interrupted",class:"badge-warning",icon:"alert-circle"};case"failed":return{label:"Capture: Failed",class:"badge-error",icon:"close-circle"};case"expired":return{label:"Capture: Expired",class:"badge-muted",icon:"clock-alert-outline"};default:return{label:`Capture: ${i||"unknown"}`,class:"badge-muted",icon:"help-circle"}}}function R(i){if(!i)return{label:"Analysis: not requested",class:"badge-muted",icon:"minus-circle-outline"};let e=typeof i=="object"?i.status:i;if(!e)return{label:"Analysis: not requested",class:"badge-muted",icon:"minus-circle-outline"};switch(e){case"complete":return{label:"Analysis: Complete",class:"badge-success",icon:"check-decagram"};case"processing":case"running":return{label:"Analysis: Processing",class:"badge-running",icon:"progress-clock"};case"pending":return{label:"Analysis: Pending",class:"badge-info",icon:"timer-sand"};case"failed":return{label:"Analysis: Failed",class:"badge-error",icon:"alert-octagon"};case"skipped":return{label:"Analysis: Skipped",class:"badge-muted",icon:"minus-circle"};case"retry_wait":return{label:"Analysis: Retry Wait",class:"badge-warning",icon:"clock-fast"};case"stale":return{label:"Analysis: Stale",class:"badge-muted",icon:"history"};case"not_ready":return{label:"Analysis: Not Ready",class:"badge-muted",icon:"minus-circle-outline"};default:return{label:`Analysis: ${e}`,class:"badge-muted",icon:"help-circle"}}}function T(i){return Array.isArray(i)?[...i].sort((e,t)=>{let n=new Date(e.occurred_at||0).getTime(),r=new Date(t.occurred_at||0).getTime();return n-r}):[]}function $(i){if(typeof i!="number"||isNaN(i))return"";if(i<60)return`${i.toFixed(1)}s`;let e=Math.floor(i/60),t=(i%60).toFixed(0);return`${e}m ${t}s`}function m(i){if(!i)return"Unknown error occurred";if(typeof i=="string")return i.replace(/<[^>]*>/g,"").replace(/(https?:\/\/[^\s]+)/g,"[redacted-url]").replace(/([A-Za-z]:\\[^\s]+)/g,"[file-path]").replace(/(token=[a-zA-Z0-9_\-]+)/g,"token=[redacted]").slice(0,160);if(typeof i=="object"){if(i.message)return m(i.message);if(i.code)return`Error code: ${y(i.code)}`}return"Operation failed"}function P(i){if(!i||typeof i!="object")return null;let e=!!(i.is_uncertain??i.uncertainty),t=Number(i.gaps??i.gap_seconds??0);return e||t>0?i.message?`\u26A0\uFE0F Coverage Uncertainty: ${i.message}`:t>0?`\u26A0\uFE0F Coverage Uncertainty: Recording gap of ${t}s detected`:"\u26A0\uFE0F Coverage Uncertainty: Source timestamps not verified":null}async function z(i,{source_id:e,kind:t,limit:n=20,before_cursor:r}={}){if(!i||!i.callWS)throw new Error("Home Assistant connection not ready");let o={type:"event_recording/events",limit:Math.max(1,Math.min(100,Number(n)||20))};e&&(o.source_id=e),t&&(o.kind=t),r&&(o.before_cursor=r);try{let c=await i.callWS(o);return{events:Array.isArray(c?.events)?c.events:[],next_cursor:c?.next_cursor||null,capabilities:c?.capabilities||{consumers:[]}}}catch(c){throw new Error(m(c))}}async function H(i,e){if(!i||!i.callWS)throw new Error("Home Assistant connection not ready");if(!e)throw new Error("Missing event_id");let t={type:"event_recording/event",event_id:e};try{return(await i.callWS(t))?.event||null}catch(n){throw new Error(m(n))}}async function U(i,e){if(!i||!i.connection||!i.connection.subscribeMessage)throw new Error("Home Assistant connection not ready");let t={type:"event_recording/subscribe"};try{let r=await i.connection.subscribeMessage(e,t);return async()=>{try{typeof r=="function"&&await r()}catch{}}}catch(n){throw new Error(m(n))}}async function j(i,{event_id:e,media_id:t,thumbnail_id:n}={}){if(!i||!i.callWS)throw new Error("Home Assistant connection not ready");if(!e)throw new Error("Missing event_id");let r=!!t,o=!!n;if(r&&o||!r&&!o)throw new Error("Exactly one of media_id or thumbnail_id must be provided");let c={type:"event_recording/media",event_id:e};r?c.media_id=t:c.thumbnail_id=n;try{let a=await i.callWS(c);if(!a||!a.url)throw new Error("Media not available");return{url:a.url,expires_at:a.expires_at||null,mime_type:a.mime_type||"video/mp4"}}catch(a){throw new Error(m(a))}}async function N(i,{event_id:e,consumer:t="event_vision",retry:n=!0}){if(!i||!i.callWS)throw new Error("Home Assistant connection not ready");if(!e)throw new Error("Missing event_id");let r={type:"event_recording/process",event_id:e,consumer:t,retry:n};try{return!!(await i.callWS(r))?.accepted}catch(o){throw new Error(m(o))}}var k=class extends HTMLElement{constructor(){super(),this.attachShadow({mode:"open"}),this._config={page_size:20},this._hass=null,this._events=[],this._nextCursor=null,this._capabilities={consumers:[]},this._loading=!1,this._loadingMore=!1,this._error=null,this._liveOffline=!1,this._boundConnection=null,this._onConnectionReady=null,this._subscribed=!1,this._unsubEvents=null,this._subEpoch=0,this._contextEpoch=0,this._feedReqSeq=0,this._mediaReqSeq=0,this._selectedEvent=null,this._selectedMediaId=null,this._mediaUrl=null,this._mediaExpiresAt=null,this._mediaMimeType=null,this._mediaLoading=!1,this._mediaError=null,this._mediaPlayErrorRetried=!1,this._retryInProgress=!1,this._retryFeedback=null,this._lastFocusedElement=null}static getConfigElement(){return document.createElement("event-recording-card-editor")}static getStubConfig(){return{page_size:20}}getCardSize(){return 6}setConfig(e){if(!e)throw new Error("Invalid configuration");let t=20;if(e.page_size!==void 0&&e.page_size!==null){let c=String(e.page_size).trim();if(!/^\d+$/.test(c))throw new Error("page_size must be a finite integer between 1 and 100");let a=Number(c);if(!Number.isSafeInteger(a)||a<1||a>100)throw new Error("page_size must be a finite integer between 1 and 100");t=a}let n=this._config.source_id,r=this._config.kind,o=this._config.page_size;this._config={type:"custom:event-recording-card",...e,page_size:t},n!==this._config.source_id||r!==this._config.kind||o!==this._config.page_size?(this._contextEpoch++,this._closeDrawer(),this._cleanupSubscription(),this._events=[],this._nextCursor=null,this.isConnected&&this._hass?.connection&&(this._initialLoad(),this._setupSubscription())):this.isConnected&&this._hass?.connection&&this._events.length===0&&this._initialLoad()}set hass(e){let t=this._boundConnection,n=e?.connection;this._hass=e,t&&t!==n&&(typeof t.removeEventListener=="function"&&this._onConnectionReady&&t.removeEventListener("ready",this._onConnectionReady),this._cleanupSubscription(),this._boundConnection=null,this._onConnectionReady=null),n&&this._boundConnection!==n&&this._bindConnection(n)}_bindConnection(e){!e||this._boundConnection===e||(this._boundConnection&&typeof this._boundConnection.removeEventListener=="function"&&this._onConnectionReady&&this._boundConnection.removeEventListener("ready",this._onConnectionReady),this._boundConnection=e,this._onConnectionReady=()=>{this.isConnected&&this._boundConnection===e&&(this._initialLoad(),this._setupSubscription())},typeof e.addEventListener=="function"&&e.addEventListener("ready",this._onConnectionReady),this.isConnected&&(this._events.length===0&&this._initialLoad(),this._setupSubscription()))}connectedCallback(){this._render(),!this._boundConnection&&this._hass?.connection?this._bindConnection(this._hass.connection):this._boundConnection&&(this._events.length===0&&this._initialLoad(),this._setupSubscription())}disconnectedCallback(){this._contextEpoch++,this._cleanupSubscription(),this._boundConnection&&(typeof this._boundConnection.removeEventListener=="function"&&this._onConnectionReady&&this._boundConnection.removeEventListener("ready",this._onConnectionReady),this._boundConnection=null,this._onConnectionReady=null)}async _setupSubscription(){if(this._subscribed||!this._boundConnection||!this.isConnected)return;let e=++this._subEpoch;try{let t=await U(this._hass,n=>this._handleLiveMessage(n));if(!this.isConnected||this._subEpoch!==e){typeof t=="function"&&await t();return}this._unsubEvents=t,this._subscribed=!0,this._liveOffline&&(this._liveOffline=!1,this._updateHeaderBanner())}catch{this._subEpoch===e&&this.isConnected&&(this._liveOffline=!0,this._updateHeaderBanner())}}_cleanupSubscription(){if(this._subEpoch++,this._subscribed=!1,typeof this._unsubEvents=="function"){let e=this._unsubEvents;this._unsubEvents=null;try{e()}catch{}}}async _initialLoad(){let e=this._contextEpoch,t=++this._feedReqSeq;this._loading=!0,this._error=null,this._render();try{let n=await z(this._hass,{source_id:this._config.source_id,kind:this._config.kind,limit:this._config.page_size});if(this._contextEpoch!==e||this._feedReqSeq!==t)return;this._events=n.events||[],this._nextCursor=n.next_cursor||null,this._capabilities=n.capabilities||{consumers:[]},this._loading=!1,this._renderFeed()}catch(n){if(this._contextEpoch!==e||this._feedReqSeq!==t)return;this._loading=!1,this._error=m(n),this._renderFeed()}}async _loadMore(){if(this._loadingMore||!this._nextCursor)return;let e=this._contextEpoch;this._loadingMore=!0,this._updateLoadMoreButton();try{let t=await z(this._hass,{source_id:this._config.source_id,kind:this._config.kind,limit:this._config.page_size,before_cursor:this._nextCursor});if(this._contextEpoch!==e)return;let n=t.events||[],r=new Set(this._events.map(o=>o.event_id));for(let o of n)r.has(o.event_id)||this._events.push(o);this._nextCursor=t.next_cursor||null,t.capabilities&&(this._capabilities=t.capabilities),this._loadingMore=!1,this._renderFeed()}catch(t){if(this._contextEpoch!==e)return;this._loadingMore=!1,this._updateLoadMoreButton(m(t))}}async _handleLiveMessage(e){if(!e||!e.event_id)return;let t=this._contextEpoch,n=this._boundConnection;try{let r=null;try{r=await H(this._hass,e.event_id)}catch(c){if(!this.isConnected||this._contextEpoch!==t||this._boundConnection!==n)return;let a=(c?.message||String(c)).toLowerCase();(a.includes("404")||a.includes("not found")||a.includes("403")||a.includes("forbidden")||a.includes("unauthorized")||a.includes("revoked"))&&this._removeEvent(e.event_id);return}if(!this.isConnected||this._contextEpoch!==t||this._boundConnection!==n)return;if(!r){this._removeEvent(e.event_id);return}let o=this._events.findIndex(c=>c.event_id===e.event_id);if(o!==-1){let c=this._events[o];if(c.revision!==void 0&&r.revision!==void 0&&r.revision<c.revision)return;this._events[o]=r}else{if(this._config.source_id&&r.source_id!==this._config.source_id||this._config.kind&&r.kind!==this._config.kind)return;this._events.unshift(r),this._events.length>this._config.page_size*2&&this._events.pop()}this._selectedEvent&&this._selectedEvent.event_id===e.event_id&&(this._selectedEvent=r,this._renderDrawerContent()),this._renderFeed()}catch{}}_removeEvent(e){let t=this._events.findIndex(n=>n.event_id===e);t!==-1&&this._events.splice(t,1),this._selectedEvent&&this._selectedEvent.event_id===e&&this._closeDrawer(),this._renderFeed()}_render(){if(!this.shadowRoot)return;let e=this._config.source_id?`Source: ${C(this._config.source_id)}`:"",t=this._config.kind?`Kind: ${w(this._config.kind)}`:"",n=[e,t].filter(Boolean).join(" \u2022 ");this.shadowRoot.innerHTML=`
      <style>${A}</style>
      <ha-card class="ev-card-container">
        <div class="ev-header">
          <div class="ev-header-title-wrap">
            <h2 class="ev-title">Events</h2>
            ${n?`<span class="ev-subtitle">${y(n)}</span>`:""}
          </div>
          <div class="ev-header-actions">
            <button class="ev-icon-button" id="refreshBtn" title="Refresh Feed" aria-label="Refresh Feed">
              \u21BB Refresh
            </button>
          </div>
        </div>

        <div id="bannerContainer"></div>
        <div id="feedContainer" class="ev-feed" role="feed" aria-busy="${this._loading}"></div>
        <div id="drawerContainer"></div>
      </ha-card>
    `;let r=this.shadowRoot.querySelector("#refreshBtn");r&&r.addEventListener("click",()=>{this._initialLoad()}),this._updateHeaderBanner(),this._renderFeed()}_updateHeaderBanner(){let e=this.shadowRoot?.querySelector("#bannerContainer");if(e)if(this._liveOffline){e.innerHTML=`
        <div class="ev-banner ev-banner-offline" role="alert">
          <span>Live updates unavailable. Reconnecting...</span>
          <button class="ev-icon-button" id="reconnectLiveBtn">Reconnect</button>
        </div>
      `;let t=e.querySelector("#reconnectLiveBtn");t&&t.addEventListener("click",()=>{this._setupSubscription()})}else e.innerHTML=""}_renderFeed(){let e=this.shadowRoot?.querySelector("#feedContainer");if(!e)return;if(this._loading&&this._events.length===0){e.innerHTML=`
        <div class="ev-loading-box">
          <div class="ev-spinner" aria-label="Loading"></div>
          <span>Loading recorded events...</span>
        </div>
      `;return}if(this._error&&this._events.length===0){e.innerHTML=`
        <div class="ev-error-box" role="alert">
          <p>Failed to load events: ${y(this._error)}</p>
          <button class="ev-icon-button" id="retryFeedBtn">Retry</button>
        </div>
      `;let r=e.querySelector("#retryFeedBtn");r&&r.addEventListener("click",()=>this._initialLoad());return}if(this._events.length===0){e.innerHTML=`
        <div class="ev-empty-box">
          <p>No recorded events found.</p>
        </div>
      `;return}let t=this._hass?.config?.time_zone;e.innerHTML="";let n=document.createElement("div");if(n.className="ev-events-list",this._events.forEach(r=>{let o=document.createElement("div");o.className="ev-row",o.tabIndex=0,o.setAttribute("role","article"),o.setAttribute("data-event-id",r.event_id),o.setAttribute("aria-label",`Event ${r.event_id}`);let c=document.createElement("div");c.className="ev-col-time";let a=document.createElement("span");a.className="ev-time-text",a.textContent=S(r.start_at,t),c.appendChild(a);let u=document.createElement("div");u.className="ev-col-info";let g=document.createElement("div");g.className="ev-event-title",g.textContent=L(r);let _=document.createElement("div");_.className="ev-event-meta";let l=document.createElement("span");l.className="ev-source-badge",l.textContent=C(r.source_id,r.label),_.appendChild(l);let p=document.createElement("span");p.className="ev-kind-badge",p.textContent=w(r.kind),_.appendChild(p);let h=r.source_events||[];if(h.length>0){let x=document.createElement("span");x.className="ev-triggers-text",x.textContent=h.map(B=>M(B.type,B.label)).filter(Boolean).join(" \u2022 "),_.appendChild(x)}u.appendChild(g),u.appendChild(_);let s=document.createElement("div");s.className="ev-col-badges";let d=D(r.capture_state),f=document.createElement("span");f.className=`ev-status-badge ${d.class}`,f.textContent=d.label,s.appendChild(f);let v=R(r.annotations?.event_vision),b=document.createElement("span");b.className=`ev-status-badge ${v.class}`,b.textContent=v.label,s.appendChild(b),o.appendChild(c),o.appendChild(u),o.appendChild(s),o.addEventListener("click",()=>this._openDrawer(r)),o.addEventListener("keydown",x=>{(x.key==="Enter"||x.key===" ")&&(x.preventDefault(),this._openDrawer(r))}),n.appendChild(o)}),e.appendChild(n),this._nextCursor){let r=document.createElement("div");r.className="ev-load-more-wrap";let o=document.createElement("button");o.className="ev-load-more-btn",o.id="loadMoreBtn",o.textContent=this._loadingMore?"Loading more...":"Load More Events",o.disabled=this._loadingMore,o.addEventListener("click",()=>this._loadMore()),r.appendChild(o),e.appendChild(r)}}_updateLoadMoreButton(e){let t=this.shadowRoot?.querySelector("#loadMoreBtn");t&&(t.disabled=this._loadingMore,this._loadingMore?t.textContent="Loading more...":e?t.textContent=`Retry Load More (${e})`:t.textContent="Load More Events")}_openDrawer(e){this._lastFocusedElement=this.shadowRoot?.activeElement,this._selectedEvent=e;let t=e.media||[],n=t.find(r=>r.available);this._selectedMediaId=n?n.id:t.length>0?t[0].id:null,this._mediaForId=null,this._mediaUrl=null,this._mediaExpiresAt=null,this._mediaMimeType=null,this._mediaError=null,this._mediaPlayErrorRetried=!1,this._retryInProgress=!1,this._retryFeedback=null,this._renderDrawerContainer(),this._selectedMediaId&&this._loadSelectedMedia(this._selectedMediaId)}_closeDrawer(){this._selectedEvent=null,this._selectedMediaId=null,this._mediaForId=null,this._mediaUrl=null,this._mediaExpiresAt=null,this._mediaMimeType=null,this._mediaError=null,this._mediaPlayErrorRetried=!1;let e=this.shadowRoot?.querySelector("#drawerContainer");if(e&&(e.innerHTML=""),this._lastFocusedElement&&typeof this._lastFocusedElement.focus=="function")try{this._lastFocusedElement.focus()}catch{}}async _loadSelectedMedia(e,t=!1){if(!this._selectedEvent)return;let n=++this._mediaReqSeq,r=this._selectedEvent.event_id,c=(this._selectedEvent.media||[]).find(a=>a.id===e);if(c&&!c.available){this._mediaUrl=null,this._mediaError="Media unavailable",this._renderDrawerContent();return}if(!t&&this._mediaForId===e&&this._mediaUrl&&this._mediaExpiresAt){let a=new Date(this._mediaExpiresAt).getTime();if(!isNaN(a)&&a>Date.now()+15e3)return}this._mediaForId=e,this._mediaUrl=null,this._mediaLoading=!0,this._mediaError=null,this._renderDrawerContent();try{let a=await j(this._hass,{event_id:r,media_id:e});if(this._mediaReqSeq!==n||!this._selectedEvent||this._selectedEvent.event_id!==r)return;this._mediaUrl=a.url,this._mediaExpiresAt=a.expires_at,this._mediaMimeType=a.mime_type,this._mediaLoading=!1,this._renderDrawerContent()}catch(a){if(this._mediaReqSeq!==n||!this._selectedEvent||this._selectedEvent.event_id!==r)return;this._mediaLoading=!1;let u=(a?.message||String(a)).toLowerCase();if(u.includes("403")||u.includes("404")||u.includes("revoked")||u.includes("forbidden")||u.includes("unauthorized")||u.includes("not found")){this._removeEvent(r);return}this._mediaError=m(a),this._renderDrawerContent()}}_renderDrawerContainer(){let e=this.shadowRoot?.querySelector("#drawerContainer");if(!e||!this._selectedEvent)return;e.innerHTML=`
      <div class="ev-drawer-backdrop" id="drawerBackdrop">
        <section class="ev-drawer" role="dialog" aria-modal="true" aria-label="Event Details">
          <div class="ev-drawer-header">
            <div class="ev-drawer-title-group">
              <h3 class="ev-drawer-title" id="drawerTitle"></h3>
              <span class="ev-drawer-sub" id="drawerSub"></span>
            </div>
            <button class="ev-icon-button ev-drawer-close-btn" id="closeDrawerBtn" aria-label="Close details">
              \u2715
            </button>
          </div>
          <div class="ev-drawer-content" id="drawerContent"></div>
        </section>
      </div>
    `;let t=e.querySelector("#drawerBackdrop"),n=e.querySelector("#closeDrawerBtn");t&&t.addEventListener("click",r=>{r.target===t&&this._closeDrawer()}),n&&n.addEventListener("click",()=>this._closeDrawer()),this.shadowRoot?.addEventListener("keydown",r=>{r.key==="Escape"&&this._selectedEvent&&this._closeDrawer()}),this._renderDrawerContent()}_renderDrawerContent(){let e=this.shadowRoot?.querySelector("#drawerContent"),t=this.shadowRoot?.querySelector("#drawerTitle"),n=this.shadowRoot?.querySelector("#drawerSub");if(!e||!this._selectedEvent)return;let r=this._selectedEvent,o=this._hass?.config?.time_zone;t&&(t.textContent=L(r)),n&&(n.textContent=`${C(r.source_id,r.label)} \u2022 ${I(r.start_at,o)}`);let c=r.media||[],a=c.find(s=>s.available),u=c.find(s=>s.id===this._selectedMediaId)||a||c[0],g=e.querySelector("video.ev-video-player"),_=e.querySelector(".ev-video-container"),l=!1;if(g&&_&&u&&u.kind==="video"&&this._mediaUrl&&!this._mediaLoading&&!this._mediaError)try{let s=new URL(g.src,window.location.href).href,d=new URL(this._mediaUrl,window.location.href).href;s===d&&(l=!0)}catch{g.src.includes(this._mediaUrl)&&(l=!0)}if(l){let s=e.querySelector(".ev-media-section");if(s){for(;s.nextSibling;)e.removeChild(s.nextSibling);this._renderPostMediaSections(e,r);return}}e.innerHTML="";let p=document.createElement("div");if(p.className="ev-media-section",c.length>1){let s=document.createElement("div");s.className="ev-clip-tabs",c.forEach((d,f)=>{let v=document.createElement("button");v.className=`ev-clip-tab ${d.id===this._selectedMediaId?"active":""}`;let b=d.kind==="image"?"Image":"Clip",x=d.duration?` (${$(d.duration)})`:d.kind==="image"?" (Snapshot)":"";v.textContent=`${b} ${f+1}${x}`,v.addEventListener("click",()=>{this._selectedMediaId=d.id,this._loadSelectedMedia(d.id)}),s.appendChild(v)}),p.appendChild(s)}let h=document.createElement("div");if(h.className="ev-video-container",c.length===0)h.innerHTML=`
        <div class="ev-media-placeholder">
          <span>No finalized media clips recorded for this incident</span>
        </div>
      `;else if(this._mediaLoading)h.innerHTML=`
        <div class="ev-media-placeholder">
          <div class="ev-spinner" aria-label="Loading media"></div>
          <span>Loading media asset...</span>
        </div>
      `;else if(this._mediaError||u&&!u.available){h.innerHTML=`
        <div class="ev-media-placeholder">
          <span>${y(this._mediaError||"Media unavailable")}</span>
          <button class="ev-media-retry-btn" id="retryMediaBtn">Retry</button>
        </div>
      `;let s=h.querySelector("#retryMediaBtn");s&&s.addEventListener("click",()=>{this._selectedMediaId&&this._loadSelectedMedia(this._selectedMediaId,!0)})}else if(this._mediaUrl)if(u&&u.kind==="image"){let s=document.createElement("img");s.className="ev-image-preview",s.src=this._mediaUrl,s.alt="Recorded incident snapshot",s.addEventListener("error",()=>{this._mediaPlayErrorRetried||(this._mediaPlayErrorRetried=!0,this._loadSelectedMedia(this._selectedMediaId,!0))}),h.appendChild(s)}else{let s=document.createElement("video");s.className="ev-video-player",s.controls=!0,s.playsInline=!0,s.autoplay=!1,s.preload="metadata",s.src=this._mediaUrl,s.addEventListener("error",()=>{this._mediaPlayErrorRetried||(this._mediaPlayErrorRetried=!0,this._loadSelectedMedia(this._selectedMediaId,!0))}),h.appendChild(s)}p.appendChild(h),e.appendChild(p),this._renderPostMediaSections(e,r)}_renderPostMediaSections(e,t){let n=this._hass?.config?.time_zone,r=P(t.coverage);if(r){let l=document.createElement("div");l.className="ev-uncertainty-box",l.setAttribute("role","alert"),l.textContent=r,e.appendChild(l)}if(t.capture_error){let l=document.createElement("div");l.className="ev-capture-error-box",l.setAttribute("role","alert");let p=m(t.capture_error);l.textContent=`\u26A0\uFE0F Capture Issue: ${p}`,e.appendChild(l)}let o=document.createElement("div");o.className="ev-detail-section";let c=document.createElement("h4");c.className="ev-section-title",c.textContent="Analysis",o.appendChild(c);let a=t.annotations?.event_vision;if(a){let l=R(a),p=document.createElement("div");if(p.className=`ev-status-badge ${l.class}`,p.style.display="inline-block",p.style.marginBottom="8px",p.textContent=l.label,o.appendChild(p),a.summary){let d=document.createElement("p");d.className="ev-summary-text",d.textContent=a.summary,o.appendChild(d)}if(typeof a.activity=="boolean"){let d=document.createElement("p");d.className="ev-activity-text",d.textContent=`Activity: ${a.activity?"Visible activity detected":"No visible activity detected"}`,o.appendChild(d)}if(Array.isArray(a.objects)&&a.objects.length>0){let d=document.createElement("p");d.className="ev-objects-text",d.textContent=`Objects: ${a.objects.join(", ")}`,o.appendChild(d)}if(a.error_code){let d=document.createElement("p");d.className="ev-error-text",d.textContent=`Error: ${a.error_code}`,o.appendChild(d)}let h=!!this._capabilities?.consumers?.some(d=>d.id==="event_vision"&&d.available&&d.can_retry);if(!!this._hass?.user?.is_admin&&h){let d=a.status==="pending"||a.status==="processing"||a.status==="running",f=document.createElement("div");f.className="ev-drawer-actions";let v=document.createElement("button");if(v.className="ev-retry-btn",v.textContent=this._retryInProgress?"Queuing Retry...":d?"Analysis in Progress":"Re-run Visual Analysis",v.disabled=this._retryInProgress||d,v.addEventListener("click",async()=>{this._retryInProgress=!0,this._retryFeedback=null,v.disabled=!0,v.textContent="Queuing Retry...";try{await N(this._hass,{event_id:t.event_id,consumer:"event_vision",retry:!0}),this._retryFeedback="Analysis retry queued successfully"}catch(b){this._retryFeedback=`Retry failed: ${m(b)}`}finally{this._retryInProgress=!1,this._renderDrawerContent()}}),f.appendChild(v),this._retryFeedback){let b=document.createElement("div");b.className="helper-text",b.textContent=this._retryFeedback,f.appendChild(b)}o.appendChild(f)}}else{let l=document.createElement("p");l.className="ev-text-muted",l.textContent="Analysis: not requested",o.appendChild(l);let p=!!this._capabilities?.consumers?.some(s=>s.id==="event_vision"&&s.available&&s.can_retry);if(!!this._hass?.user?.is_admin&&p){let s=document.createElement("div");s.className="ev-drawer-actions";let d=document.createElement("button");if(d.className="ev-retry-btn",d.textContent=this._retryInProgress?"Queuing Request...":"Run Visual Analysis",d.disabled=this._retryInProgress,d.addEventListener("click",async()=>{this._retryInProgress=!0,this._retryFeedback=null,d.disabled=!0,d.textContent="Queuing Request...";try{await N(this._hass,{event_id:t.event_id,consumer:"event_vision",retry:!1}),this._retryFeedback="Analysis request queued successfully"}catch(f){this._retryFeedback=`Request failed: ${m(f)}`}finally{this._retryInProgress=!1,this._renderDrawerContent()}}),s.appendChild(d),this._retryFeedback){let f=document.createElement("div");f.className="helper-text",f.textContent=this._retryFeedback,s.appendChild(f)}o.appendChild(s)}}e.appendChild(o);let u=document.createElement("div");u.className="ev-detail-section";let g=document.createElement("h4");g.className="ev-section-title",g.textContent="Source Chronology",u.appendChild(g);let _=T(t.source_events);if(_.length===0){let l=document.createElement("p");l.className="ev-text-muted",l.textContent="No sensor trigger events recorded",u.appendChild(l)}else{let l=document.createElement("ul");l.className="ev-chronology",_.forEach(p=>{let h=document.createElement("li");h.className="ev-chronology-item";let s=document.createElement("span");s.className="ev-chronology-type",s.textContent=M(p.type,p.label);let d=document.createElement("span");d.className="ev-chronology-offset",d.textContent=F(p.occurred_at,t.start_at);let f=document.createElement("span");f.className="ev-chronology-time",f.textContent=S(p.occurred_at,n),h.appendChild(s),h.appendChild(d),h.appendChild(f),l.appendChild(h)}),u.appendChild(l)}e.appendChild(u)}};customElements.get("event-recording-card")||customElements.define("event-recording-card",k);window.customCards=window.customCards||[];window.customCards.push({type:"event-recording-card",name:"Event Recording Card",description:"A Home Assistant Lovelace custom card for recorded events and optional vision annotations.",preview:!0});export{k as EventRecordingCard,E as EventRecordingCardEditor};
