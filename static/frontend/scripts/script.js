/* Scripts files and functions */

var close_btn = document.getElementsByClassName('close-btn')[0];
var content_alert = document.getElementsByClassName('content-alert')[0];

if(close_btn) {
    close_btn.addEventListener('click', function() {
        content_alert.style.display = "none";
    });
}
