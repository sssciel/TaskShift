const toastContainer = document.getElementById('toastContainer');
const TOAST_DEFAULT_TIMEOUT = 3500;
const TOAST_ERROR_TIMEOUT = 6000;

function showToast(message, kind = 'info', duration = null) {
  if (!toastContainer) {
    return;
  }
  const toast = document.createElement('div');
  toast.className = `toast ${kind}`;
  toast.textContent = message;
  toastContainer.appendChild(toast);
  const timeout = duration || (kind === 'error' ? TOAST_ERROR_TIMEOUT : TOAST_DEFAULT_TIMEOUT);
  const dismissHandle = window.setTimeout(() => dismissToast(toast), timeout);
  toast.addEventListener('click', () => {
    window.clearTimeout(dismissHandle);
    dismissToast(toast);
  });
  return toast;
}

function dismissToast(toast) {
  if (!toast || !toast.parentNode) {
    return;
  }
  toast.classList.add('leaving');
  toast.addEventListener('animationend', () => {
    if (toast.parentNode) {
      toast.parentNode.removeChild(toast);
    }
  });
}
