/* UPDATE THESE VALUES TO MATCH YOUR SETUP */
const PROCESSING_STATS_API_URL = "http://localhost:8100/stats"
const ANALYZER_API_URL = {
    stats: "http://localhost:8110/stats",
    playerSnapshot: "http://localhost:8110/player-snapshot?index=0",
    matchEvent: "http://localhost:8110/match-event?index=0"
}

// This function fetches and updates the general statistics
const makeReq = (url, cb) => {
    fetch(url)
        .then(res => res.json())
        .then((result) => {
            console.log("Received data: ", result)
            cb(result);
        }).catch((error) => {
            updateErrorMessages(error.message)
        })
}
 
const updateCodeDiv = (result, elemId) => document.getElementById(elemId).innerText = JSON.stringify(result)

const prettyKey = (k) => k
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())

const renderEvent = (result, elemId) => {
    const el = document.getElementById(elemId)
    if (result.message) {
        el.innerHTML = `<p>${result.message}</p>`
        return
    }
    el.innerHTML = Object.entries(result)
        .map(([k, v]) => `<div class="event-row"><span class="event-key">${prettyKey(k)}:</span> <span class="event-val">${v ?? 'N/A'}</span></div>`)
        .join("")
}
 
const getLocaleDateStr = () => (new Date()).toLocaleString()
 
const getStats = () => {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr()
    
    makeReq(PROCESSING_STATS_API_URL, (result) => renderEvent(result, "processing-stats"))
    makeReq(ANALYZER_API_URL.stats, (result) => renderEvent(result, "analyzer-stats"))
    makeReq(ANALYZER_API_URL.playerSnapshot, (result) => renderEvent(result, "event-player-snapshot"))
    makeReq(ANALYZER_API_URL.matchEvent, (result) => renderEvent(result, "event-match-event"))
}
 
const updateErrorMessages = (message) => {
    const id = Date.now()
    console.log("Creation", id)
    msg = document.createElement("div")
    msg.id = `error-${id}`
    msg.innerHTML = `<p>Something happened at ${getLocaleDateStr()}!</p><code>${message}</code>`
    document.getElementById("messages").style.display = "block"
    document.getElementById("messages").prepend(msg)
    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`)
        if (elem) { elem.remove() }
    }, 7000)
}
 
const setup = () => {
    getStats()
    setInterval(() => getStats(), 4000) // Update every 4 seconds
}
 
document.addEventListener('DOMContentLoaded', setup)