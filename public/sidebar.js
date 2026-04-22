/**
 * Shared sidebar + auth helpers.
 * Every protected page should:
 *   1. Include <link rel="stylesheet" href="/css/tokens.css">
 *   2. Include <script src="/sidebar.js"></script> (usually at end of body)
 *   3. Have <main class="main"><div class="topbar">...</div><div class="page-content">...</div></main>
 *      at the top level of <body>.
 *   4. Set <body data-page="connectors"> (or whatever nav key to highlight)
 */
(function () {
  const token = sessionStorage.getItem('skc_admin_token');
  const userStr = sessionStorage.getItem('skc_admin_user');

  if (!token || !userStr) {
    window.location.href = '/login.html';
    return;
  }

  let user;
  try { user = JSON.parse(userStr); }
  catch (e) { logout(); return; }

  function logout() {
    sessionStorage.removeItem('skc_admin_token');
    sessionStorage.removeItem('skc_admin_user');
    window.location.href = '/login.html';
  }

  async function api(method, path, body) {
    const opts = {
      method,
      headers: { 'Authorization': 'Bearer ' + token },
    };
    if (body !== undefined) {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }
    const res = await fetch(path, opts);
    if (res.status === 401) { logout(); throw new Error('Session expired'); }
    let data = {};
    try { data = await res.json(); } catch (e) {}
    if (!res.ok) throw new Error(data.error || ('HTTP ' + res.status));
    return data;
  }

  function loadSidebar() {
    return fetch('/sidebar.html')
      .then((r) => r.text())
      .then((html) => {
        const host = document.getElementById('sidebar-host') || document.body;
        const wrap = document.createElement('div');
        wrap.innerHTML = html;
        if (host === document.body) {
          host.insertBefore(wrap.firstElementChild, host.firstChild);
        } else {
          host.appendChild(wrap.firstElementChild);
        }

        // Set active nav
        const page = document.body.getAttribute('data-page');
        if (page) {
          const activeLink = document.querySelector('.nav-item[data-nav="' + page + '"]');
          if (activeLink) activeLink.classList.add('active');
        }

        // Show user email
        const sbUser = document.getElementById('sbUser');
        if (sbUser) sbUser.textContent = user.email || '';

        document.dispatchEvent(new CustomEvent('sidebarReady'));
      });
  }

  window._SKCAdmin = { user, token, api, logout, loadSidebar };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', loadSidebar);
  } else {
    loadSidebar();
  }
})();
