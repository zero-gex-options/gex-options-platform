// Navigation initialization - run after _navigation.html is loaded

function initializeNavigation() {
    // Auto-highlight active page based on current URL
    const currentPath = window.location.pathname;
    const pathMap = {
        '/': 'home',
        '/gamma': 'gamma',
        '/put-call': 'put-call',
        '/flows': 'flows',
        '/spy-price': 'spy-price',
        '/market-bias': 'market-bias'
    };

    const activePage = pathMap[currentPath];
    if (activePage) {
        const activeLink = document.querySelector(`[data-page="${activePage}"]`);
        if (activeLink) {
            activeLink.classList.add('active');
        }
    }

    // Update clock every second
    function updateClock() {
        const clockElement = document.getElementById('headerClock');
        if (!clockElement) return;

        const now = new Date();
        const dateTimeString = now.toLocaleString('en-US', {
            timeZone: 'America/New_York',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        });

        // Convert from "MM/DD/YYYY, HH:MM:SS" to "YYYY-MM-DD HH:MM:SS"
        const parts = dateTimeString.split(', ');
        const dateParts = parts[0].split('/');
        const timePart = parts[1];
        const formatted = `${dateParts[2]}-${dateParts[0]}-${dateParts[1]} ${timePart}`;

        clockElement.textContent = formatted;
    }

    updateClock();
    setInterval(updateClock, 1000);
}
