// URL for Processing service stats endpoint
const PROCESSING_STATS_API_URL = "http://localhost:8100/stats"

// URLs for Analyzer service endpoints
const ANALYZER_API_URL = {
    stats: "http://localhost:8110/stats",
    playerSnapshot: "http://localhost:8110/player-snapshot",
    matchEvent: "http://localhost:8110/match-event"
}

// Generic function to make a GET request to a URL
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
 
// const updateCodeDiv = (result, elemId) => document.getElementById(elemId).innerText = JSON.stringify(result)

const prettyKey = (k) => k
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())

// Renders JSON data nicely into the UI
const renderEvent = (result, elemId) => {
    const el = document.getElementById(elemId)

    if (result.message) {
        el.innerHTML = `<p>${result.message}</p>`
        return
    }

    el.innerHTML = Object.entries(result)
        .map(([k, v]) => 
            `<div class="event-row">
                <span class="event-key">${prettyKey(k)}:</span> 
                <span class="event-val">${v ?? 'N/A'}</span>
            </div>`)
        .join("")
}
 
const getLocaleDateStr = () => (new Date()).toLocaleString()

const getRandomIndex = () => Math.floor(Math.random() * 100)
 
// // Main function that fetches ALL data and updates the UI
// const getStats = () => {
//     document.getElementById("last-updated-value").innerText = getLocaleDateStr()
    
//     makeReq(PROCESSING_STATS_API_URL, (result) => renderEvent(result, "processing-stats"))
//     makeReq(ANALYZER_API_URL.stats, (result) => renderEvent(result, "analyzer-stats"))
//     makeReq(`${ANALYZER_API_URL.playerSnapshot}?index=${getRandomIndex()}`, (result) => renderEvent(result, "event-player-snapshot"))
//     makeReq(`${ANALYZER_API_URL.matchEvent}?index=${getRandomIndex()}`, (result) => renderEvent(result, "event-match-event"))
// }

const getStats = () => {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr()
    
    makeReq(PROCESSING_STATS_API_URL, (result) => renderEvent(result, "processing-stats"))
    
    makeReq(ANALYZER_API_URL.stats, (result) => {
        renderEvent(result, "analyzer-stats")

        const playerCount = result.num_player_snapshots
        const matchCount = result.num_match_events

        if (playerCount > 0) {
            const playerIndex = Math.floor(Math.random() * playerCount)
            makeReq(`${ANALYZER_API_URL.playerSnapshot}?index=${playerIndex}`, (result) => renderEvent(result, "event-player-snapshot"))
        }

        if (matchCount > 0) {
            const matchIndex = Math.floor(Math.random() * matchCount)
            makeReq(`${ANALYZER_API_URL.matchEvent}?index=${matchIndex}`, (result) => renderEvent(result, "event-match-event"))
        }
    })
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