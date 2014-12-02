// Escapes Javascript Strings adding the proper number of \
function es(string) {
    string = string.replace(/\\/g, "\\\\");
    string = string.replace(/["]/g, "\\\"");
    return string;
}

// Ensures that the String fits on maxlen. Otherwise, cut and put ...
function cut2(string, maxlen) {
    if (string.length > maxlen - 3) {
        return string.substring(0, Math.max(0, maxlen - 3)) + "...";
    } else {
        return string;
    }
}

//Ensures that the String fits on maxlen. Otherwise, cut and put ... in the middle
function cut(string, maxlen) {
    if (string.length > maxlen - 3) {
        var half = (maxlen - 3) / 2;
        return string.substring(0, Math.max(0, half)) + "..." + string.substring(string.length - half, string.length);
    } else {
        return string;
    }
}


// Get the params from the url
$.urlParam = function (name) {
    var results = new RegExp('[\\?&]' + name + '=([^&#]*)').exec(window.location.href);
    return results[1] || 0;
}