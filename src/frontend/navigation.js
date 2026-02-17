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
        '/market-bias': 'market-bias',
        '/max-pain': 'max-pain',
        '/about': 'about'
    };
    const activePage = pathMap[currentPath];
    if (activePage) {
        const activeLink = document.querySelector(`[data-page="${activePage}"]`);
        if (activeLink) {
            activeLink.classList.add('active');
        }
    }

    // Initialize theme toggle
    const themeToggle = document.getElementById('themeToggle');
    const themeLabel = document.getElementById('themeLabel');

    if (themeToggle && themeLabel) {
        // Load saved theme
        const savedTheme = localStorage.getItem('theme') || 'dark';
        applyTheme(savedTheme);

        themeToggle.addEventListener('click', () => {
            const currentTheme = document.body.classList.contains('light-mode') ? 'light' : 'dark';
            const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
            applyTheme(newTheme);
            localStorage.setItem('theme', newTheme);
        });
    }

    function applyTheme(theme) {
        if (theme === 'light') {
            document.body.classList.add('light-mode');
            themeToggle.classList.add('light');
            themeLabel.textContent = 'Light';

            // Switch to light logos (use querySelectorAll for multiple elements)
            const headerLogos = document.querySelectorAll('.header-logo');
            const headerHelmets = document.querySelectorAll('.header-helmet');
            const footerLogos = document.querySelectorAll('.footer-logo');

            headerLogos.forEach(logo => logo.src = '/logo_title_light');
            headerHelmets.forEach(helmet => helmet.src = '/logo_icon_light');
            footerLogos.forEach(logo => logo.src = '/logo_full_light');
        } else {
            document.body.classList.remove('light-mode');
            themeToggle.classList.remove('light');
            themeLabel.textContent = 'Dark';

            // Switch to dark logos (use querySelectorAll for multiple elements)
            const headerLogos = document.querySelectorAll('.header-logo');
            const headerHelmets = document.querySelectorAll('.header-helmet');
            const footerLogos = document.querySelectorAll('.footer-logo');

            headerLogos.forEach(logo => logo.src = '/logo_title');
            headerHelmets.forEach(helmet => helmet.src = '/logo_icon');
            footerLogos.forEach(logo => logo.src = '/logo_full');
        }
    }

    // Initialize dropdown functionality
    const dropdown = document.getElementById('symbolDropdown');
    const dropdownToggle = document.getElementById('symbolDropdownToggle');

    if (dropdown && dropdownToggle) {
        dropdownToggle.addEventListener('click', (e) => {
            e.stopPropagation();
            dropdown.classList.toggle('open');
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', () => {
            dropdown.classList.remove('open');
        });

        // Handle symbol selection
        document.querySelectorAll('.nav-dropdown-item').forEach(item => {
            item.addEventListener('click', (e) => {
                e.stopPropagation();
                const symbol = item.getAttribute('data-symbol');

                // Update selected symbol display
                document.getElementById('selectedSymbol').textContent = symbol;

                // Update selected state
                document.querySelectorAll('.nav-dropdown-item').forEach(i => {
                    i.classList.remove('selected');
                });
                item.classList.add('selected');

                // Close dropdown
                dropdown.classList.remove('open');

                // Store selected symbol in localStorage for future use
                localStorage.setItem('selectedSymbol', symbol);

                // Log for now (in future this will reload data for the selected symbol)
                console.log('Selected symbol:', symbol);
            });
        });

        // Load saved symbol on page load
        const savedSymbol = localStorage.getItem('selectedSymbol') || 'SPY';
        document.getElementById('selectedSymbol').textContent = savedSymbol;
        document.querySelectorAll('.nav-dropdown-item').forEach(item => {
            if (item.getAttribute('data-symbol') === savedSymbol) {
                item.classList.add('selected');
            }
        });
    }

    // Update analog clock every second
    function updateAnalogClock() {
        const hourHand = document.getElementById('hourHand');
        const minuteHand = document.getElementById('minuteHand');
        const secondHand = document.getElementById('secondHand');

        if (!hourHand || !minuteHand || !secondHand) return;

        const now = new Date();
        const etTime = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));

        const hours = etTime.getHours() % 12;
        const minutes = etTime.getMinutes();
        const seconds = etTime.getSeconds();

        // Calculate angles (0 degrees = 12 o'clock, clockwise)
        const secondAngle = (seconds * 6) - 90; // 6 degrees per second
        const minuteAngle = (minutes * 6 + seconds * 0.1) - 90; // 6 degrees per minute + smooth seconds
        const hourAngle = (hours * 30 + minutes * 0.5) - 90; // 30 degrees per hour + smooth minutes

        // Apply rotations
        secondHand.setAttribute('transform', `rotate(${secondAngle} 50 50)`);
        minuteHand.setAttribute('transform', `rotate(${minuteAngle} 50 50)`);
        hourHand.setAttribute('transform', `rotate(${hourAngle} 50 50)`);
    }

    updateAnalogClock();
    setInterval(updateAnalogClock, 1000);
}
