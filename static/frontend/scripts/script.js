/* Scripts files and functions */

var close_btn = document.getElementsByClassName('close-btn')[0];
var content_alert = document.getElementsByClassName('content-alert')[0];
const $number_tasks = document.getElementById('task_number');
const $gee_form = document.getElementById('gee_form');

/* Modal parameters */
var modal = document.getElementById('downloadModal');
var btn = document.querySelector('.btn-modal');
var span = document.querySelector('.close');
const modalTitle = document.getElementById('modalTitle');
const modalList = document.getElementById('modalList');

document.addEventListener('DOMContentLoaded', function () {

    if (close_btn) {
        close_btn.addEventListener('click', function () {
            content_alert.style.display = "none";
        });
    }

    if ($gee_form) {
        (function () {
            $gee_form.addEventListener('submit', (e) => {
                if (!(Number($number_tasks.value) >= 1 && Number($number_tasks.value) <= 5)) {
                    alert('el valor ingresado no es permitido');
                    e.preventDefault();
                }
            });
        })();
    }

    if (btn) {

        document.querySelectorAll(".btn-modal").forEach(btn => {

            btn.addEventListener("click", function () {

                const dir = this.dataset.dir;
                const update = this.dataset.update;
                const uptime = this.dataset.uptime;
                const files = this.dataset.files.split("|");

                modalTitle.innerHTML = "<center><p class='titledir'>" + dir + "</p></center>" + " <br> Fecha actualización: " + update + " " + uptime;

                modalList.innerHTML = "";
                files.forEach(file => {
                    const li = document.createElement("li");
                    li.textContent = file;
                    modalList.appendChild(li);
                });

                modal.style.display = "block";
                modal.style.display = "flex";


            });
            
        });

        span.addEventListener("click", function () {
            modal.style.display = "none";
        });

        window.addEventListener("click", function (e) {
            if (e.target === modal) {
                modal.style.display = "none";
            }
        });
    }

});
