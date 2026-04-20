const loginLanguageSelect = document.getElementById('loginLanguageSelect');
const loginTranslations = window.__TASKSHIFT_LOGIN_TRANSLATIONS__ || {};

function getLoginLanguage() {
  const stored = localStorage.getItem('taskshift-admin-language');
  if (stored === 'ru' || stored === 'en') {
    return stored;
  }
  return (navigator.language || '').toLowerCase().startsWith('ru') ? 'ru' : 'en';
}

function applyLoginLanguage(language) {
  const tr = loginTranslations[language] || loginTranslations.en || {};
  document.documentElement.lang = language;
  document.getElementById('loginTitle').textContent = tr.title || '';
  document.getElementById('loginDescription').innerHTML = tr.description || '';
  document.getElementById('loginTokenLabel').textContent = tr.tokenLabel || '';
  document.getElementById('loginSubmitButton').textContent = tr.submit || '';
  const errorNode = document.getElementById('loginError');
  if ((errorNode.textContent || '').trim() === ((loginTranslations.en || {}).invalidToken || '')) {
    errorNode.textContent = tr.invalidToken || '';
  }
  loginLanguageSelect.value = language;
}

const loginLanguage = getLoginLanguage();
applyLoginLanguage(loginLanguage);
loginLanguageSelect.addEventListener('change', () => {
  localStorage.setItem('taskshift-admin-language', loginLanguageSelect.value);
  applyLoginLanguage(loginLanguageSelect.value);
});
