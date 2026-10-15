var z=`
  :host {
    display: block;
    --ev-card-bg: var(--ha-card-background, var(--card-background-color, #fff));
    --ev-text-primary: var(--primary-text-color, #212121);
    --ev-text-secondary: var(--secondary-text-color, #626262);
    --ev-border: var(--ha-card-border-color, var(--divider-color, #ddd));
    --ev-primary: var(--primary-color, #03a9f4);
    --ev-error: var(--error-color, #db4437);
    --ev-warning: var(--warning-color, #b77800);
    font-family: var(--paper-font-body1_-_font-family, inherit);
    color: var(--ev-text-primary);
  }
  *, *::before, *::after { box-sizing: border-box; }
  [hidden] { display: none !important; }
  ha-card { display: block; overflow: hidden; background: var(--ev-card-bg); color: var(--ev-text-primary); }
  button, select { font: inherit; color: inherit; }
  button { cursor: pointer; }
  button:disabled { cursor: default; opacity: .45; }
  button:focus-visible, select:focus-visible, summary:focus-visible { outline: 2px solid var(--ev-primary); outline-offset: -2px; }
  .ev-header { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 12px 16px; }
  .ev-header-title-wrap { min-width: 0; }
  .ev-title { font-size: 1rem; font-weight: 600; margin: 0; }
  .ev-subtitle { display: block; font-size: .75rem; color: var(--ev-text-secondary); margin-top: 4px; overflow-wrap: anywhere; }
  .ev-icon-button { min-width: 40px; min-height: 40px; padding: 6px 10px; border: 0; background: transparent; border-radius: 8px; font-size: 1.2rem; }
  .ev-icon-button:hover:not(:disabled), .ev-row:hover, .ev-load-more-btn:hover { background: rgba(128,128,128,.09); }
  .ev-events-list { display: flex; flex-direction: column; }
  .ev-row { border: 0; border-top: 1px solid var(--ev-border); background: transparent; padding: 14px 16px; display: flex; align-items: center; gap: 12px; text-align: left; width: 100%; min-height: 84px; }
  .ev-col-info { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 5px; }
  .ev-event-title { font-size: .9rem; font-weight: 500; line-height: 1.4; overflow-wrap: anywhere; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
  .ev-event-meta { font-size: .75rem; line-height: 1.4; color: var(--ev-text-secondary); overflow-wrap: anywhere; }
  .ev-feed-notice { font-size: .75rem; color: var(--ev-warning); }
  .ev-thumbnail { flex: 0 0 76px; width: 76px; height: 56px; border-radius: 7px; background: rgba(128,128,128,.1); display: flex; align-items: center; justify-content: center; color: var(--ev-text-secondary); overflow: hidden; }
  .ev-thumbnail img { display: block; width: 100%; height: 100%; object-fit: cover; }
  .ev-load-more-wrap { border-top: 1px solid var(--ev-border); padding: 8px 12px; display: flex; justify-content: center; flex-wrap: wrap; gap: 6px; }
  .ev-load-more-btn { min-height: 40px; padding: 6px 10px; border: 0; border-radius: 6px; background: transparent; color: var(--ev-primary); font-size: .85rem; }
  .ev-empty-box, .ev-loading-box { padding: 24px 16px; color: var(--ev-text-secondary); text-align: center; font-size: .85rem; }
  .ev-loading-box { display: flex; align-items: center; justify-content: center; gap: 10px; }
  .ev-banner { padding: 10px 16px; font-size: .85rem; display: flex; align-items: center; justify-content: space-between; gap: 10px; background: rgba(128,128,128,.09); }
  .ev-error-box, .ev-error-text { padding: 12px 16px; color: var(--ev-error); font-size: .85rem; overflow-wrap: anywhere; }
  .ev-drawer-backdrop { width: min(640px, calc(100vw - 32px)); max-width: none; max-height: calc(100dvh - 32px); margin: auto; padding: 0; border: 1px solid var(--ev-border); border-radius: 14px; background: var(--ev-card-bg); color: var(--ev-text-primary); box-shadow: 0 16px 64px rgba(0,0,0,.3); }
  .ev-drawer-backdrop::backdrop { background: rgba(0,0,0,.55); }
  .ev-drawer { display: flex; flex-direction: column; }
  .ev-drawer-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; padding: 16px; border-bottom: 1px solid var(--ev-border); background: var(--ev-card-bg); }
  .ev-drawer-title-group { min-width: 0; }
  .ev-drawer-title { font-size: 1rem; font-weight: 500; line-height: 1.4; margin: 0; overflow-wrap: anywhere; }
  .ev-drawer-sub { display: block; font-size: .75rem; color: var(--ev-text-secondary); margin-top: 5px; }
  .ev-drawer-content { padding: 16px; display: flex; flex-direction: column; gap: 12px; }
  .ev-media-section { width: 100%; }
  .ev-video-container { background: #000; border-radius: 8px; overflow: hidden; position: relative; min-height: 160px; display: flex; align-items: center; justify-content: center; }
  .ev-video-player, .ev-image-preview { display: block; width: 100%; height: auto; max-height: 55dvh; object-fit: contain; }
  .ev-media-placeholder { display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 180px; gap: 12px; padding: 20px; text-align: center; color: #eee; font-size: .85rem; }
  .ev-media-retry-btn { min-height: 44px; border: 1px solid #999; border-radius: 6px; background: #333; color: #fff; padding: 8px 14px; }
  .ev-play-fallback { position: absolute; inset: auto auto 60px; }
  .ev-clip-navigation { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
  .ev-clip-select { background: var(--ev-card-bg); color: var(--ev-text-secondary); border: 0; border-radius: 6px; min-height: 44px; max-width: calc(100% - 100px); min-width: 0; font-size: .8rem; padding: 6px; }
  .ev-primary-summary { font-size: .95rem; line-height: 1.5; margin: 4px 0; overflow-wrap: anywhere; }
  .ev-trigger-sequence, .ev-coverage-note { color: var(--ev-text-secondary); font-size: .78rem; line-height: 1.5; margin: 0; overflow-wrap: anywhere; }
  .ev-recording-details { border-top: 1px solid var(--ev-border); padding-top: 8px; font-size: .8rem; color: var(--ev-text-secondary); }
  .ev-recording-details > summary { cursor: pointer; min-height: 44px; padding: 12px 0; }
  .ev-detail-section { padding: 8px 0; }
  .ev-section-title { font-size: .8rem; font-weight: 500; margin: 4px 0 8px; }
  .ev-activity-text, .ev-objects-text, .ev-text-muted, .helper-text { font-size: .8rem; line-height: 1.5; margin: 6px 0; overflow-wrap: anywhere; }
  .ev-status-badge { display: inline-block; font-size: .75rem; }
  .ev-chronology { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 8px; }
  .ev-chronology-item { display: flex; flex-wrap: wrap; gap: 8px; font-size: .75rem; }
  .ev-chronology-type { flex: 1; }
  .ev-uncertainty-box, .ev-capture-error-box { padding: 10px 12px; border-radius: 6px; background: rgba(255,152,0,.12); color: var(--ev-text-primary); font-size: .85rem; line-height: 1.5; overflow-wrap: anywhere; }
  .ev-capture-error-box { color: var(--ev-error); background: rgba(244,67,54,.1); }
  .ev-drawer-actions { margin: 10px 0; }
  .ev-retry-btn { font-size: .8rem; padding: 10px 12px; min-height: 44px; border: 1px solid var(--ev-border); border-radius: 6px; background: transparent; }
  .ev-spinner { width: 20px; height: 20px; border: 2px solid var(--ev-border); border-top-color: var(--ev-primary); border-radius: 50%; animation: ev-spin .8s linear infinite; }
  @keyframes ev-spin { to { transform: rotate(360deg); } }
  @media (max-width: 480px) {
    .ev-row { padding: 12px; gap: 10px; }
    .ev-header { padding: 10px 12px; }
    .ev-thumbnail { flex-basis: 64px; width: 64px; height: 52px; }
    .ev-drawer-backdrop { width: 100%; max-height: 100dvh; margin: auto 0 0; border-radius: 14px 14px 0 0; }
    .ev-drawer-content, .ev-drawer-header { padding: 12px; }
  }
  @media (pointer: coarse) { .ev-icon-button, .ev-load-more-btn { min-height: 44px; min-width: 44px; } }
  @media (prefers-reduced-motion: reduce) { .ev-spinner { animation: none; } }
`,A=`
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
`;var w=class extends HTMLElement{constructor(){super(),this.attachShadow({mode:"open"}),this._config={},this._hass=null}setConfig(e){this._config={...e},this._render()}set hass(e){this._hass=e}connectedCallback(){this._render()}_valueChanged(e,t){if(!this._config)return;let r={...this._config};if(t===void 0||t==="")delete r[e];else if(e==="page_size"){let d=parseInt(t,10);r[e]=isNaN(d)?20:Math.max(1,Math.min(100,d))}else e==="preview_count"?r[e]=Math.max(1,Math.min(20,Number(t)||3)):typeof t=="boolean"?r[e]=t:r[e]=t.trim();this._config=r;let a=new CustomEvent("config-changed",{detail:{config:this._config},bubbles:!0,composed:!0});this.dispatchEvent(a)}_render(){if(!this.shadowRoot)return;let e=this._config.page_size!==void 0?this._config.page_size:20,t=this._config.source_id||"",r=this._config.kind||"";this.shadowRoot.innerHTML=`
      <style>${A}</style>
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
            value="${this._escape(r)}"
          />
          <div class="helper-text">Filter feed events to a specific incident kind or leave blank for all kinds.</div>
        </div>

        <div class="form-group">
          <label for="preview_count">Recent events (1 \u2013 20)</label>
          <input id="preview_count" type="number" min="1" max="20" step="1" value="${Number(this._config.preview_count)||3}" />
        </div>
        <div class="form-group">
          <label><input id="hide_no_activity" type="checkbox" ${this._config.hide_no_activity!==!1?"checked":""} /> Hide events with no visible activity</label>
          <div class="helper-text">Only current completed analysis can hide an event. Pending, failed, or missing analysis remains visible.</div>
        </div>
        <div class="form-group">
          <label><input id="show_journal_events" type="checkbox" ${this._config.show_journal_events===!0?"checked":""} /> Include journal-only events</label>
          <div class="helper-text">Show completed events that have no recording or image.</div>
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
    `;let a=this.shadowRoot.querySelector("#source_id"),d=this.shadowRoot.querySelector("#kind"),c=this.shadowRoot.querySelector("#page_size");a&&a.addEventListener("change",n=>this._valueChanged("source_id",n.target.value)),d&&d.addEventListener("change",n=>this._valueChanged("kind",n.target.value)),c&&c.addEventListener("change",n=>this._valueChanged("page_size",n.target.value)),this.shadowRoot.querySelector("#preview_count")?.addEventListener("change",n=>this._valueChanged("preview_count",n.target.value));for(let n of["hide_no_activity","show_journal_events"])this.shadowRoot.querySelector(`#${n}`)?.addEventListener("change",o=>this._valueChanged(n,o.target.checked))}_escape(e){return e==null?"":String(e).replace(/"/g,"&quot;")}};customElements.get("event-recording-card-editor")||customElements.define("event-recording-card-editor",w);function x(i){return i==null?"":String(i).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#39;")}function E(i,e){return e&&typeof e=="string"&&e.trim()?e.trim():!i||typeof i!="string"?"Source":i.split(/[-_]/).filter(Boolean).map(t=>t.charAt(0).toUpperCase()+t.slice(1).toLowerCase()).join(" ")||"Source"}function y(i,e){return e&&typeof e=="string"&&e.trim()?e.trim():!i||typeof i!="string"?"Event":i.split(/[-_]/).filter(Boolean).map(t=>t.charAt(0).toUpperCase()+t.slice(1).toLowerCase()).join(" ")||"Event"}function I(i,e){if(!i)return"--:--:--";try{let t=new Date(i);if(isNaN(t.getTime()))return String(i);let r={hour:"2-digit",minute:"2-digit",second:"2-digit",hour12:!1};return e&&(r.timeZone=e),new Intl.DateTimeFormat(void 0,r).format(t)}catch{return String(i)}}function $(i,e){if(!i)return"";try{let t=new Date(i);if(isNaN(t.getTime()))return String(i);let r={year:"numeric",month:"short",day:"numeric",hour:"2-digit",minute:"2-digit",second:"2-digit",timeZoneName:"short"};return e&&(r.timeZone=e),new Intl.DateTimeFormat(void 0,r).format(t)}catch{return String(i)}}function F(i,e){if(!i||!e)return"+0s";let t=new Date(e).getTime(),r=new Date(i).getTime();if(isNaN(t)||isNaN(r))return"+0s";let a=Math.round((r-t)/1e3);return a>=0?`+${a}s`:`${a}s`}function k(i,e){return{contact_open:"Door opened",contact_close:"Door closed",person:"Person detected",motion:"Motion",button:"Button pressed"}[i]||(i?y(i):y(i,e))}function C(i){let e=i?.annotations?.event_vision;return!e||e.status!=="complete"||e.media_revision!==void 0&&i.media_revision!==void 0&&e.media_revision!==i.media_revision?null:e}function T(i,e={}){return!(e.hide_no_activity!==!1&&C(i)?.activity===!1||e.show_journal_events!==!0&&!i.media?.length&&!i.capture_error&&["ready","expired"].includes(i.capture_state))}function B(i){return i.capture_error||["failed","interrupted"].includes(i.capture_state)?"Recording interrupted":Number(i.coverage?.gaps??i.coverage?.gap_seconds??0)>0?"Video has gaps":i.capture_state==="partial"&&!(i.coverage?.is_uncertain??i.coverage?.uncertainty)?"Video incomplete":["requested","recording"].includes(i.capture_state)?"Recording\u2026":i.capture_state==="expired"||i.media?.length&&!i.media.some(e=>e.available)?"Video unavailable":i.annotations?.event_vision?.status==="failed"?"Summary unavailable":""}function D(i,e){if(!i||!Number.isFinite(new Date(i).getTime()))return"";let t={hour:"2-digit",minute:"2-digit",hour12:!1,...e?{timeZone:e}:{}},r=new Intl.DateTimeFormat(void 0,{year:"numeric",month:"numeric",day:"numeric",...e?{timeZone:e}:{}}),a=new Date(i),d=new Intl.DateTimeFormat(void 0,t).format(a);return r.format(a)===r.format(new Date)?d:`${new Intl.DateTimeFormat(void 0,{day:"numeric",month:"short",...e?{timeZone:e}:{}}).format(a)}, ${d}`}function P(i){if(!i)return"Incident recorded";let e=C(i),t=e?.title||e?.summary;if(typeof t=="string"&&t.trim()){let r=t.trim().split(/(?<=[.!?])\s/)[0];return r.length>120?`${r.slice(0,117).trimEnd()}\u2026`:r}return i.label&&typeof i.label=="string"&&i.label.trim()?i.label.trim():i.kind&&typeof i.kind=="string"&&i.kind.trim()?`${y(i.kind)} recorded`:"Incident recorded"}function j(i){switch(i){case"ready":return{label:"Capture: Ready",class:"badge-success",icon:"check-circle"};case"recording":return{label:"Capture: Recording",class:"badge-recording",icon:"record-rec"};case"requested":return{label:"Capture: Requested",class:"badge-info",icon:"clock-outline"};case"partial":return{label:"Capture: Partial",class:"badge-warning",icon:"alert-circle-outline"};case"interrupted":return{label:"Capture: Interrupted",class:"badge-warning",icon:"alert-circle"};case"failed":return{label:"Capture: Failed",class:"badge-error",icon:"close-circle"};case"expired":return{label:"Capture: Expired",class:"badge-muted",icon:"clock-alert-outline"};default:return{label:`Capture: ${i||"unknown"}`,class:"badge-muted",icon:"help-circle"}}}function M(i){if(!i)return{label:"Analysis: not requested",class:"badge-muted",icon:"minus-circle-outline"};let e=typeof i=="object"?i.status:i;if(!e)return{label:"Analysis: not requested",class:"badge-muted",icon:"minus-circle-outline"};switch(e){case"complete":return{label:"Analysis: Complete",class:"badge-success",icon:"check-decagram"};case"processing":case"running":return{label:"Analysis: Processing",class:"badge-running",icon:"progress-clock"};case"pending":return{label:"Analysis: Pending",class:"badge-info",icon:"timer-sand"};case"failed":return{label:"Analysis: Failed",class:"badge-error",icon:"alert-octagon"};case"skipped":return{label:"Analysis: Skipped",class:"badge-muted",icon:"minus-circle"};case"retry_wait":return{label:"Analysis: Retry Wait",class:"badge-warning",icon:"clock-fast"};case"stale":return{label:"Analysis: Stale",class:"badge-muted",icon:"history"};case"not_ready":return{label:"Analysis: Not Ready",class:"badge-muted",icon:"minus-circle-outline"};default:return{label:`Analysis: ${e}`,class:"badge-muted",icon:"help-circle"}}}function L(i){return Array.isArray(i)?[...i].sort((e,t)=>{let r=new Date(e.occurred_at||0).getTime(),a=new Date(t.occurred_at||0).getTime();return r-a}):[]}function H(i){if(typeof i!="number"||isNaN(i))return"";if(i<60)return`${i.toFixed(1)}s`;let e=Math.floor(i/60),t=(i%60).toFixed(0);return`${e}m ${t}s`}function _(i){if(!i)return"Unknown error occurred";if(typeof i=="string")return i.replace(/<[^>]*>/g,"").replace(/(https?:\/\/[^\s]+)/g,"[redacted-url]").replace(/([A-Za-z]:\\[^\s]+)/g,"[file-path]").replace(/(token=[a-zA-Z0-9_\-]+)/g,"token=[redacted]").slice(0,160);if(typeof i=="object"){if(i.message)return _(i.message);if(i.code)return`Error code: ${x(i.code)}`}return"Operation failed"}function U(i){if(!i||typeof i!="object")return null;let e=!!(i.is_uncertain??i.uncertainty),t=Number(i.gaps??i.gap_seconds??0);return e||t>0?i.message?`\u26A0\uFE0F Coverage Uncertainty: ${i.message}`:t>0?`\u26A0\uFE0F Coverage Uncertainty: Recording gap of ${t}s detected`:"\u26A0\uFE0F Coverage Uncertainty: Source timestamps not verified":null}async function R(i,{source_id:e,kind:t,limit:r=20,before_cursor:a}={}){if(!i||!i.callWS)throw new Error("Home Assistant connection not ready");let d={type:"event_recording/events",limit:Math.max(1,Math.min(100,Number(r)||20))};e&&(d.source_id=e),t&&(d.kind=t),a&&(d.before_cursor=a);try{let c=await i.callWS(d);return{events:Array.isArray(c?.events)?c.events:[],next_cursor:c?.next_cursor||null,capabilities:c?.capabilities||{consumers:[]}}}catch(c){throw new Error(_(c))}}async function O(i,e){if(!i||!i.callWS)throw new Error("Home Assistant connection not ready");if(!e)throw new Error("Missing event_id");let t={type:"event_recording/event",event_id:e};try{return(await i.callWS(t))?.event||null}catch(r){throw new Error(_(r))}}async function W(i,e){if(!i||!i.connection||!i.connection.subscribeMessage)throw new Error("Home Assistant connection not ready");let t={type:"event_recording/subscribe"};try{let a=await i.connection.subscribeMessage(e,t);return async()=>{try{typeof a=="function"&&await a()}catch{}}}catch(r){throw new Error(_(r))}}async function N(i,{event_id:e,media_id:t,thumbnail_id:r}={}){if(!i||!i.callWS)throw new Error("Home Assistant connection not ready");if(!e)throw new Error("Missing event_id");let a=!!t,d=!!r;if(a&&d||!a&&!d)throw new Error("Exactly one of media_id or thumbnail_id must be provided");let c={type:"event_recording/media",event_id:e};a?c.media_id=t:c.thumbnail_id=r;try{let n=await i.callWS(c);if(!n||!n.url)throw new Error("Media not available");return{url:n.url,expires_at:n.expires_at||null,mime_type:n.mime_type||"video/mp4"}}catch(n){throw new Error(_(n))}}async function q(i,{event_id:e,consumer:t="event_vision",retry:r=!0}){if(!i||!i.callWS)throw new Error("Home Assistant connection not ready");if(!e)throw new Error("Missing event_id");let a={type:"event_recording/process",event_id:e,consumer:t,retry:r};try{return!!(await i.callWS(a))?.accepted}catch(d){throw new Error(_(d))}}var S=class extends HTMLElement{constructor(){super(),this.attachShadow({mode:"open"}),this._config={page_size:20,preview_count:3,hide_no_activity:!0,show_journal_events:!1},this._expanded=!1,this._detailsOpen=!1,this._thumbnailCache=new Map,this._hass=null,this._events=[],this._nextCursor=null,this._capabilities={consumers:[]},this._loading=!1,this._loadingMore=!1,this._error=null,this._liveOffline=!1,this._boundConnection=null,this._onConnectionReady=null,this._subscribed=!1,this._unsubEvents=null,this._subEpoch=0,this._contextEpoch=0,this._feedReqSeq=0,this._mediaReqSeq=0,this._selectedEvent=null,this._selectedMediaId=null,this._mediaUrl=null,this._mediaExpiresAt=null,this._mediaMimeType=null,this._mediaLoading=!1,this._mediaError=null,this._mediaPlayErrorRetried=!1,this._retryInProgress=!1,this._retryFeedback=null,this._lastFocusedElement=null}static getConfigElement(){return document.createElement("event-recording-card-editor")}static getStubConfig(){return{page_size:20,preview_count:3,hide_no_activity:!0}}getCardSize(){return 4}setConfig(e){if(!e)throw new Error("Invalid configuration");let t=20;if(e.page_size!==void 0&&e.page_size!==null){let n=String(e.page_size).trim();if(!/^\d+$/.test(n))throw new Error("page_size must be a finite integer between 1 and 100");let o=Number(n);if(!Number.isSafeInteger(o)||o<1||o>100)throw new Error("page_size must be a finite integer between 1 and 100");t=o}let r=e.preview_count??3;if(!Number.isInteger(r)||r<1||r>20)throw new Error("preview_count must be an integer between 1 and 20");for(let n of["hide_no_activity","show_journal_events"])if(e[n]!==void 0&&typeof e[n]!="boolean")throw new Error(`${n} must be a boolean`);let a=this._config.source_id,d=this._config.kind,c=this._config.page_size;this._config={type:"custom:event-recording-card",hide_no_activity:!0,show_journal_events:!1,...e,preview_count:r,page_size:t},a!==this._config.source_id||d!==this._config.kind||c!==this._config.page_size?(this._contextEpoch++,this._closeDrawer(),this._cleanupSubscription(),this._events=[],this._nextCursor=null,this._loadingMore=!1,this._thumbnailCache.clear(),this.isConnected&&this._hass?.connection&&(this._initialLoad(),this._setupSubscription())):this.isConnected&&this._hass?.connection&&this._events.length===0?this._initialLoad():this._renderFeed()}set hass(e){let t=this._boundConnection,r=e?.connection;this._hass=e,t&&t!==r&&(typeof t.removeEventListener=="function"&&this._onConnectionReady&&t.removeEventListener("ready",this._onConnectionReady),this._cleanupSubscription(),this._boundConnection=null,this._onConnectionReady=null),r&&this._boundConnection!==r&&this._bindConnection(r)}_bindConnection(e){!e||this._boundConnection===e||(this._boundConnection&&typeof this._boundConnection.removeEventListener=="function"&&this._onConnectionReady&&this._boundConnection.removeEventListener("ready",this._onConnectionReady),this._boundConnection=e,this._onConnectionReady=()=>{this.isConnected&&this._boundConnection===e&&(this._initialLoad(),this._setupSubscription())},typeof e.addEventListener=="function"&&e.addEventListener("ready",this._onConnectionReady),this.isConnected&&(this._events.length===0&&this._initialLoad(),this._setupSubscription()))}connectedCallback(){this._render(),!this._boundConnection&&this._hass?.connection?this._bindConnection(this._hass.connection):this._boundConnection&&(this._events.length===0&&this._initialLoad(),this._setupSubscription())}disconnectedCallback(){this._contextEpoch++,this._closeDrawer(),this._thumbnailCache.clear(),this._cleanupSubscription(),this._boundConnection&&(typeof this._boundConnection.removeEventListener=="function"&&this._onConnectionReady&&this._boundConnection.removeEventListener("ready",this._onConnectionReady),this._boundConnection=null,this._onConnectionReady=null)}async _setupSubscription(){if(this._subscribed||!this._boundConnection||!this.isConnected)return;let e=++this._subEpoch;try{let t=await W(this._hass,r=>this._handleLiveMessage(r));if(!this.isConnected||this._subEpoch!==e){typeof t=="function"&&await t();return}this._unsubEvents=t,this._subscribed=!0,this._liveOffline&&(this._liveOffline=!1,this._updateHeaderBanner())}catch{this._subEpoch===e&&this.isConnected&&(this._liveOffline=!0,this._updateHeaderBanner())}}_cleanupSubscription(){if(this._subEpoch++,this._subscribed=!1,typeof this._unsubEvents=="function"){let e=this._unsubEvents;this._unsubEvents=null;try{e()}catch{}}}async _initialLoad(){let e=this._contextEpoch,t=++this._feedReqSeq;this._loading=!0,this._loadingMore=!1,this._thumbnailCache.clear(),this._expanded=!1,this._closeDrawer(),this._error=null,this._render();try{let r=await R(this._hass,{source_id:this._config.source_id,kind:this._config.kind,limit:this._config.page_size});if(this._contextEpoch!==e||this._feedReqSeq!==t)return;this._events=r.events||[],this._nextCursor=r.next_cursor||null,this._capabilities=r.capabilities||{consumers:[]},this._loading=!1,this._renderFeed()}catch(r){if(this._contextEpoch!==e||this._feedReqSeq!==t)return;this._loading=!1,this._error=_(r),this._renderFeed()}}async _loadMore(){if(this._loadingMore||!this._nextCursor)return;let e=this._contextEpoch,t=this._feedReqSeq;this._loadingMore=!0,this._updateLoadMoreButton();try{let r=await R(this._hass,{source_id:this._config.source_id,kind:this._config.kind,limit:this._config.page_size,before_cursor:this._nextCursor});if(this._contextEpoch!==e||this._feedReqSeq!==t)return;let a=r.events||[],d=new Set(this._events.map(c=>c.event_id));for(let c of a)d.has(c.event_id)||this._events.push(c);this._nextCursor=r.next_cursor||null,r.capabilities&&(this._capabilities=r.capabilities),this._loadingMore=!1,this._renderFeed()}catch(r){if(this._contextEpoch!==e||this._feedReqSeq!==t)return;this._loadingMore=!1,this._updateLoadMoreButton(_(r))}}async _handleLiveMessage(e){if(!e||!e.event_id)return;let t=this._contextEpoch,r=this._boundConnection;try{let a=null;try{a=await O(this._hass,e.event_id)}catch(c){if(!this.isConnected||this._contextEpoch!==t||this._boundConnection!==r)return;let n=(c?.message||String(c)).toLowerCase();(n.includes("404")||n.includes("not found")||n.includes("403")||n.includes("forbidden")||n.includes("unauthorized")||n.includes("revoked"))&&this._removeEvent(e.event_id);return}if(!this.isConnected||this._contextEpoch!==t||this._boundConnection!==r)return;if(!a){this._removeEvent(e.event_id);return}let d=this._events.findIndex(c=>c.event_id===e.event_id);if(d!==-1){let c=this._events[d];if(c.revision!==void 0&&a.revision!==void 0&&a.revision<c.revision)return;this._events[d]=a}else{if(this._config.source_id&&a.source_id!==this._config.source_id||this._config.kind&&a.kind!==this._config.kind)return;this._events.unshift(a),this._events.length>Math.max(100,this._config.page_size*2)&&this._events.pop()}if(this._selectedEvent&&this._selectedEvent.event_id===e.event_id){this._selectedEvent=a;let c=a.media?.find(o=>o.id===this._selectedMediaId),n=a.media?.find(o=>o.available);!this._selectedMediaId&&n?(this._selectedMediaId=n.id,this._loadSelectedMedia(n.id)):(c&&!c.available&&this._stopPlayback(),this._renderDrawerContent())}this._renderFeed()}catch{}}_removeEvent(e){let t=this._events.findIndex(r=>r.event_id===e);t!==-1&&this._events.splice(t,1),this._selectedEvent&&this._selectedEvent.event_id===e&&this._closeDrawer(),this._renderFeed()}_render(){if(!this.shadowRoot)return;let e=this._config.source_id?`Source: ${E(this._config.source_id)}`:"",t=this._config.kind?`Kind: ${y(this._config.kind)}`:"",r=[e,t].filter(Boolean).join(" \u2022 ");this.shadowRoot.innerHTML=`
      <style>${z}</style>
      <ha-card class="ev-card-container">
        <div class="ev-header">
          <div class="ev-header-title-wrap">
            <h2 class="ev-title">Activity</h2>
            ${r?`<span class="ev-subtitle">${x(r)}</span>`:""}
          </div>
          <div class="ev-header-actions">
            <button class="ev-icon-button" id="refreshBtn" title="Refresh Feed" aria-label="Refresh Feed">
              \u21BB
            </button>
          </div>
        </div>

        <div id="bannerContainer"></div>
        <div id="feedContainer" class="ev-feed" role="feed" aria-busy="${this._loading}"></div>
        <div id="drawerContainer"></div>
      </ha-card>
    `;let a=this.shadowRoot.querySelector("#refreshBtn");a&&a.addEventListener("click",()=>{this._initialLoad()}),this._updateHeaderBanner(),this._renderFeed()}_updateHeaderBanner(){let e=this.shadowRoot?.querySelector("#bannerContainer");if(e)if(this._liveOffline){e.innerHTML=`
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
          <p>Failed to load events: ${x(this._error)}</p>
          <button class="ev-icon-button" id="retryFeedBtn">Retry</button>
        </div>
      `;let n=e.querySelector("#retryFeedBtn");n&&n.addEventListener("click",()=>this._initialLoad());return}let t=this._events.filter(n=>T(n,this._config));e.innerHTML="",e.setAttribute("aria-busy",String(this._loading));let r=this._hass?.config?.time_zone,a=document.createElement("div");a.className="ev-events-list";let d=this._expanded?t:t.slice(0,this._config.preview_count);for(let n of d){let o=document.createElement("button");o.type="button",o.className="ev-row",o.setAttribute("data-event-id",n.event_id);let v=P(n),g=E(n.source_id),l=D(n.start_at,r);o.setAttribute("aria-label",`${v}, ${l}, ${g}`);let m=document.createElement("span");m.className="ev-col-info";let p=document.createElement("span");p.className="ev-event-title",p.textContent=v;let h=document.createElement("span");h.className="ev-event-meta",h.textContent=`${l} \xB7 ${g}`,m.appendChild(p),m.appendChild(h);let s=B(n);if(s){let f=document.createElement("span");f.className="ev-feed-notice",f.textContent=s,m.appendChild(f)}o.appendChild(m);let u=document.createElement("span");u.className="ev-thumbnail",u.setAttribute("aria-hidden","true"),u.innerHTML='<ha-icon icon="mdi:play-circle-outline"></ha-icon>',o.appendChild(u),this._loadThumbnail(n,u),o.addEventListener("click",()=>this._openDrawer(n)),a.appendChild(o)}if(e.appendChild(a),!d.length){let n=document.createElement("div");n.className="ev-empty-box",n.textContent=this._config.hide_no_activity?"No activity to show.":"No recorded events found.",e.appendChild(n)}if(this._error){let n=document.createElement("p");n.className="ev-error-text",n.setAttribute("role","alert"),n.textContent=`Could not refresh activity: ${this._error}`,e.appendChild(n)}if(!!(this._nextCursor||t.length>this._config.preview_count)||this._expanded){let n=document.createElement("div");n.className="ev-load-more-wrap";let o=document.createElement("button");if(o.className="ev-load-more-btn",o.id="loadMoreBtn",o.textContent=this._expanded?"Load more activity":"View all activity",o.disabled=this._loadingMore,o.hidden=this._expanded&&!this._nextCursor,o.addEventListener("click",()=>{this._expanded?this._loadMore():(this._expanded=!0,this._renderFeed(),t.length<=this._config.preview_count&&this._nextCursor&&this._loadMore())}),n.appendChild(o),this._expanded){let v=document.createElement("button");v.className="ev-load-more-btn",v.textContent="Show recent activity",v.addEventListener("click",()=>{this._expanded=!1,this._renderFeed()}),n.appendChild(v)}e.appendChild(n)}}async _loadThumbnail(e,t){if(!e.media?.some(o=>o.available))return;let r=this._contextEpoch,a=`${e.event_id}:${e.media_revision}`,d=this._thumbnailCache.get(a);d||(d=N(this._hass,{event_id:e.event_id,thumbnail_id:"thumbnail"}).catch(()=>null),this._thumbnailCache.set(a,d),this._thumbnailCache.size>100&&this._thumbnailCache.delete(this._thumbnailCache.keys().next().value));let c=await d;if(this._contextEpoch!==r||!t.isConnected||!c)return;if(c.expires_at&&new Date(c.expires_at).getTime()<Date.now()+1e4){this._thumbnailCache.delete(a);return}let n=document.createElement("img");n.alt="",n.src=c.url,n.addEventListener("load",()=>{t.isConnected&&t.replaceChildren(n)}),n.addEventListener("error",()=>this._thumbnailCache.delete(a))}_updateLoadMoreButton(e){let t=this.shadowRoot?.querySelector("#loadMoreBtn");t&&(t.disabled=this._loadingMore,t.textContent=this._loadingMore?"Loading\u2026":e?`Retry (${e})`:"Load more activity")}_openDrawer(e){this._stopPlayback(),this._detailsOpen=!1,this._lastFocusedElement=this.shadowRoot?.activeElement,this._selectedEvent=e;let t=e.media||[],r=t.find(a=>a.available);this._selectedMediaId=r?r.id:t.length>0?t[0].id:null,this._mediaForId=null,this._mediaUrl=null,this._mediaExpiresAt=null,this._mediaMimeType=null,this._mediaError=null,this._mediaPlayErrorRetried=!1,this._retryInProgress=!1,this._retryFeedback=null,this._renderDrawerContainer(),this._selectedMediaId&&this._loadSelectedMedia(this._selectedMediaId)}_stopPlayback(){let e=this.shadowRoot?.querySelector("video.ev-video-player");e&&(e.pause(),e.removeAttribute("src"),e.load())}_closeDrawer(){this._stopPlayback(),this._mediaReqSeq++;let e=this.shadowRoot?.querySelector("dialog");e?.open&&e.close(),this._selectedEvent=null,this._selectedMediaId=null,this._mediaForId=null,this._mediaUrl=null,this._mediaExpiresAt=null,this._mediaMimeType=null,this._mediaError=null,this._mediaPlayErrorRetried=!1;let t=this.shadowRoot?.querySelector("#drawerContainer");if(t&&(t.innerHTML=""),this._lastFocusedElement&&typeof this._lastFocusedElement.focus=="function")try{this._lastFocusedElement.focus()}catch{}}async _loadSelectedMedia(e,t=!1){if(!this._selectedEvent)return;let r=++this._mediaReqSeq,a=this._selectedEvent.event_id,c=(this._selectedEvent.media||[]).find(n=>n.id===e);if(c&&!c.available){this._mediaUrl=null,this._mediaError="Media unavailable",this._renderDrawerContent();return}if(!t&&this._mediaForId===e&&this._mediaUrl&&this._mediaExpiresAt){let n=new Date(this._mediaExpiresAt).getTime();if(!isNaN(n)&&n>Date.now()+15e3)return}this._stopPlayback(),this._mediaForId=e,this._mediaUrl=null,this._mediaLoading=!0,this._mediaError=null,this._renderDrawerContent();try{let n=await N(this._hass,{event_id:a,media_id:e});if(this._mediaReqSeq!==r||!this._selectedEvent||this._selectedEvent.event_id!==a)return;this._mediaUrl=n.url,this._mediaExpiresAt=n.expires_at,this._mediaMimeType=n.mime_type,this._mediaLoading=!1,this._renderDrawerContent()}catch(n){if(this._mediaReqSeq!==r||!this._selectedEvent||this._selectedEvent.event_id!==a)return;this._mediaLoading=!1;let o=(n?.message||String(n)).toLowerCase();if(o.includes("403")||o.includes("404")||o.includes("revoked")||o.includes("forbidden")||o.includes("unauthorized")||o.includes("not found")){this._removeEvent(a);return}this._mediaError=_(n),this._renderDrawerContent()}}_renderDrawerContainer(){let e=this.shadowRoot?.querySelector("#drawerContainer");if(!e||!this._selectedEvent)return;e.innerHTML=`
      <dialog class="ev-drawer-backdrop" id="drawerBackdrop" aria-label="Event details">
        <section class="ev-drawer">
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
      </dialog>
    `;let t=e.querySelector("#drawerBackdrop"),r=e.querySelector("#closeDrawerBtn");t&&t.addEventListener("click",a=>{a.target===t&&this._closeDrawer()}),r&&r.addEventListener("click",()=>this._closeDrawer()),t?.addEventListener("cancel",a=>{a.preventDefault(),this._closeDrawer()}),typeof t?.showModal=="function"&&t.showModal(),this._renderDrawerContent()}_renderDrawerContent(){let e=this.shadowRoot?.querySelector("#drawerContent"),t=this.shadowRoot?.querySelector("#drawerTitle"),r=this.shadowRoot?.querySelector("#drawerSub");if(!e||!this._selectedEvent)return;let a=this._selectedEvent,d=this._hass?.config?.time_zone;t&&(t.textContent=E(a.source_id)),r&&(r.textContent=$(a.start_at,d));let c=a.media||[],n=c.find(s=>s.available),o=c.find(s=>s.id===this._selectedMediaId)||n||c[0],v=e.querySelector("video.ev-video-player"),g=e.querySelector(".ev-video-container"),l=!1;if(v&&g&&o&&o.available&&o.kind==="video"&&this._mediaUrl&&!this._mediaLoading&&!this._mediaError)try{let s=new URL(v.src,window.location.href).href,u=new URL(this._mediaUrl,window.location.href).href;s===u&&(l=!0)}catch{v.src.includes(this._mediaUrl)&&(l=!0)}if(l){let s=e.querySelector(".ev-media-section");if(s){for(;s.nextSibling;)e.removeChild(s.nextSibling);this._renderPostMediaSections(e,a);return}}e.innerHTML="";let m=document.createElement("div");m.className="ev-media-section";let p=document.createElement("div");if(p.className="ev-video-container",c.length===0)p.innerHTML=`
        <div class="ev-media-placeholder">
          <span>No finalized media clips recorded for this incident</span>
        </div>
      `;else if(this._mediaLoading)p.innerHTML=`
        <div class="ev-media-placeholder">
          <div class="ev-spinner" aria-label="Loading media"></div>
          <span>Loading media asset...</span>
        </div>
      `;else if(this._mediaError||o&&!o.available){p.innerHTML=`
        <div class="ev-media-placeholder">
          <span>${x(this._mediaError||"Media unavailable")}</span>
          <button class="ev-media-retry-btn" id="retryMediaBtn">Retry</button>
        </div>
      `;let s=p.querySelector("#retryMediaBtn");s&&s.addEventListener("click",()=>{this._selectedMediaId&&this._loadSelectedMedia(this._selectedMediaId,!0)})}else if(this._mediaUrl)if(o&&o.kind==="image"){let s=document.createElement("img");s.className="ev-image-preview",s.src=this._mediaUrl,s.alt="Recorded incident snapshot",s.addEventListener("error",()=>{this._mediaPlayErrorRetried||(this._mediaPlayErrorRetried=!0,this._loadSelectedMedia(this._selectedMediaId,!0))}),p.appendChild(s)}else{let s=document.createElement("video");s.className="ev-video-player",s.controls=!0,s.playsInline=!0,s.autoplay=!0,s.muted=!0,s.defaultMuted=!0,s.preload="auto",s.addEventListener("ended",()=>{if(!this._selectedEvent||this._selectedEvent.event_id!==a.event_id)return;let u=this._selectedEvent.media||[],f=u.slice(u.findIndex(b=>b.id===this._selectedMediaId)+1).find(b=>b.available);f&&(this._selectedMediaId=f.id,this._mediaPlayErrorRetried=!1,this._loadSelectedMedia(f.id))}),s.src=this._mediaUrl,s.addEventListener("error",()=>{this._mediaPlayErrorRetried||(this._mediaPlayErrorRetried=!0,this._loadSelectedMedia(this._selectedMediaId,!0))}),p.appendChild(s)}m.appendChild(p),e.appendChild(m);let h=p.querySelector("video");h&&h.play().catch(()=>{if(!h.isConnected)return;let s=document.createElement("button");s.className="ev-media-retry-btn ev-play-fallback",s.textContent="Play recording",s.addEventListener("click",()=>h.play().then(()=>s.remove()).catch(()=>{})),p.appendChild(s)}),this._renderPostMediaSections(e,a)}_renderPostMediaSections(e,t){let r=t.media||[];if(r.length>1){let l=document.createElement("div");l.className="ev-clip-navigation";let m=Math.max(0,r.findIndex(h=>h.id===this._selectedMediaId)),p=document.createElement("select");p.className="ev-clip-select",p.setAttribute("aria-label","Recording clip"),r.forEach((h,s)=>{let u=document.createElement("option");u.value=h.id,u.textContent=`${h.kind==="image"?"Image":"Clip"} ${s+1} of ${r.length}${h.duration?` \xB7 ${H(h.duration)}`:""}${h.available?"":" \xB7 unavailable"}`,u.selected=s===m,p.appendChild(u)}),p.addEventListener("change",()=>{this._selectedMediaId=p.value,this._mediaPlayErrorRetried=!1,this._loadSelectedMedia(p.value)});for(let[h,s]of[["Previous clip",-1],["Next clip",1]]){let u=document.createElement("button");u.className="ev-icon-button",u.textContent=s<0?"\u2039":"\u203A",u.setAttribute("aria-label",h),u.disabled=!r[m+s],u.addEventListener("click",()=>{this._selectedMediaId=r[m+s].id,this._mediaPlayErrorRetried=!1,this._loadSelectedMedia(this._selectedMediaId)}),l.appendChild(u),s<0&&l.appendChild(p)}e.appendChild(l)}let a=C(t),d=document.createElement("p");d.className="ev-primary-summary",d.textContent=a?.summary||(t.annotations?.event_vision?.status==="failed"?"Summary unavailable. The recording is saved.":t.annotations?.event_vision?"Preparing summary\u2026":""),d.textContent&&e.appendChild(d);let c=document.createElement("p");c.className="ev-trigger-sequence",c.textContent=[...new Set(L(t.source_events).map(l=>k(l.type,l.label)))].join(" \u2192 "),c.textContent&&e.appendChild(c);let n=document.createElement("div");this._renderDiagnostics(n,t);let o=document.createElement("details");o.className="ev-recording-details",o.open=this._detailsOpen,o.addEventListener("toggle",()=>{this._detailsOpen=o.open});let v=document.createElement("summary");v.textContent="Recording details",o.appendChild(v);for(let l of[...n.children])l.className==="ev-capture-error-box"||l.className==="ev-uncertainty-box"?(l.className==="ev-uncertainty-box"&&!(Number(t.coverage?.gaps??t.coverage?.gap_seconds??0)>0)&&(l.className="ev-coverage-note",l.removeAttribute("role"),l.textContent=r.length>1?"Source timing is unverified; continuity between clips is not guaranteed.":"Source timing is unverified."),e.appendChild(l)):o.appendChild(l);let g=document.createElement("p");g.className="ev-text-muted",g.textContent=`${j(t.capture_state).label} \xB7 ${M(t.annotations?.event_vision).label}`,o.appendChild(g),e.appendChild(o)}_renderDiagnostics(e,t){let r=this._hass?.config?.time_zone,a=U(t.coverage);if(a){let l=document.createElement("div");l.className="ev-uncertainty-box",l.setAttribute("role","alert"),l.textContent=a,e.appendChild(l)}if(t.capture_error){let l=document.createElement("div");l.className="ev-capture-error-box",l.setAttribute("role","alert");let m=_(t.capture_error);l.textContent=`\u26A0\uFE0F Capture Issue: ${m}`,e.appendChild(l)}let d=document.createElement("div");d.className="ev-detail-section";let c=document.createElement("h4");c.className="ev-section-title",c.textContent="Analysis",d.appendChild(c);let n=t.annotations?.event_vision;if(n){let l=M(n),m=document.createElement("div");if(m.className=`ev-status-badge ${l.class}`,m.style.display="inline-block",m.style.marginBottom="8px",m.textContent=l.label,d.appendChild(m),typeof n.activity=="boolean"){let s=document.createElement("p");s.className="ev-activity-text",s.textContent=`Activity: ${n.activity?"Visible activity detected":"No visible activity detected"}`,d.appendChild(s)}if(Array.isArray(n.objects)&&n.objects.length>0){let s=document.createElement("p");s.className="ev-objects-text",s.textContent=`Objects: ${n.objects.join(", ")}`,d.appendChild(s)}if(n.error_code){let s=document.createElement("p");s.className="ev-error-text",s.textContent=`Error: ${n.error_code}`,d.appendChild(s)}let p=!!this._capabilities?.consumers?.some(s=>s.id==="event_vision"&&s.available&&s.can_retry);if(!!this._hass?.user?.is_admin&&p){let s=n.status==="pending"||n.status==="processing"||n.status==="running",u=document.createElement("div");u.className="ev-drawer-actions";let f=document.createElement("button");if(f.className="ev-retry-btn",f.textContent=this._retryInProgress?"Queuing Retry...":s?"Analysis in Progress":"Re-run Visual Analysis",f.disabled=this._retryInProgress||s,f.addEventListener("click",async()=>{this._retryInProgress=!0,this._retryFeedback=null,f.disabled=!0,f.textContent="Queuing Retry...";try{await q(this._hass,{event_id:t.event_id,consumer:"event_vision",retry:!0}),this._retryFeedback="Analysis retry queued successfully"}catch(b){this._retryFeedback=`Retry failed: ${_(b)}`}finally{this._retryInProgress=!1,this._renderDrawerContent()}}),u.appendChild(f),this._retryFeedback){let b=document.createElement("div");b.className="helper-text",b.textContent=this._retryFeedback,u.appendChild(b)}d.appendChild(u)}}else{let l=document.createElement("p");l.className="ev-text-muted",l.textContent="Analysis: not requested",d.appendChild(l);let m=!!this._capabilities?.consumers?.some(h=>h.id==="event_vision"&&h.available&&h.can_retry);if(!!this._hass?.user?.is_admin&&m){let h=document.createElement("div");h.className="ev-drawer-actions";let s=document.createElement("button");if(s.className="ev-retry-btn",s.textContent=this._retryInProgress?"Queuing Request...":"Run Visual Analysis",s.disabled=this._retryInProgress,s.addEventListener("click",async()=>{this._retryInProgress=!0,this._retryFeedback=null,s.disabled=!0,s.textContent="Queuing Request...";try{await q(this._hass,{event_id:t.event_id,consumer:"event_vision",retry:!1}),this._retryFeedback="Analysis request queued successfully"}catch(u){this._retryFeedback=`Request failed: ${_(u)}`}finally{this._retryInProgress=!1,this._renderDrawerContent()}}),h.appendChild(s),this._retryFeedback){let u=document.createElement("div");u.className="helper-text",u.textContent=this._retryFeedback,h.appendChild(u)}d.appendChild(h)}}e.appendChild(d);let o=document.createElement("div");o.className="ev-detail-section";let v=document.createElement("h4");v.className="ev-section-title",v.textContent="Source Chronology",o.appendChild(v);let g=L(t.source_events);if(g.length===0){let l=document.createElement("p");l.className="ev-text-muted",l.textContent="No sensor trigger events recorded",o.appendChild(l)}else{let l=document.createElement("ul");l.className="ev-chronology",g.forEach(m=>{let p=document.createElement("li");p.className="ev-chronology-item";let h=document.createElement("span");h.className="ev-chronology-type",h.textContent=k(m.type,m.label);let s=document.createElement("span");s.className="ev-chronology-offset",s.textContent=F(m.occurred_at,t.start_at);let u=document.createElement("span");u.className="ev-chronology-time",u.textContent=I(m.occurred_at,r),p.appendChild(h),p.appendChild(s),p.appendChild(u),l.appendChild(p)}),o.appendChild(l)}e.appendChild(o)}};customElements.get("event-recording-card")||customElements.define("event-recording-card",S);window.customCards=window.customCards||[];window.customCards.push({type:"event-recording-card",name:"Event Recording Card",description:"A Home Assistant Lovelace custom card for recorded events and optional vision annotations.",preview:!0});export{S as EventRecordingCard,w as EventRecordingCardEditor};
