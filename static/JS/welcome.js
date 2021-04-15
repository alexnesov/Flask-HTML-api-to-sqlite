$('form-control').keydown(function (e) {
    if (e.keyCode == 13) {
        e.preventDefault();
        return false;
    }
    });