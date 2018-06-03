init_content = {}

function init() {
    for (let element of document.getElementsByTagName("content")) {
        init_content[element.getAttribute("cid")] = element
    }
    for (let cid in init_content) {
        let element = document.getElementById('content-' + cid)
        if (element === undefined) continue
        element.innerHTML = ''
        let content = init_content[cid]
        if (content === undefined) continue
        for (let child of content.childNodes) {
            element.appendChild(child)
        }
    }
}
