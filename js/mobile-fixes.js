// Mobile optimization script
document.addEventListener('DOMContentLoaded', function() {
  // Detect mobile
  const isMobile = window.innerWidth <= 768;
  
  if (isMobile) {
    // Add mobile class to body
    document.body.classList.add('is-mobile');
    
    // Add touch-action for better scrolling
    document.querySelectorAll('.sidebar, .map-container').forEach(el => {
      el.style.touchAction = 'pan-y';
    });
    
    // Adjust Leaflet map for mobile
    if (window.ttcApp && window.ttcApp.map) {
      // Enable touch zoom
      window.ttcApp.map.touchZoom.enable();
      window.ttcApp.map.doubleClickZoom.enable();
      
      // Set mobile-friendly bounds
      const torontoBounds = L.latLngBounds(
        [43.58, -79.63],
        [43.86, -79.12]
      );
      window.ttcApp.map.setMinZoom(10);
      window.ttcApp.map.setMaxZoom(16);
    }
    
    // Load mobile-specific CSS
    const mobileCSS = `
      .is-mobile .header-toggles {
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
      }
      
      .is-mobile .leaflet-popup-content-wrapper {
        max-width: 280px !important;
      }
      
      .is-mobile .leaflet-popup-content {
        font-size: 14px !important;
      }
    `;
    
    const style = document.createElement('style');
    style.textContent = mobileCSS;
    document.head.appendChild(style);
  }
  
  // Handle orientation changes
  window.addEventListener('orientationchange', function() {
    setTimeout(() => {
      if (window.ttcApp && window.ttcApp.map) {
        window.ttcApp.map.invalidateSize();
      }
    }, 300);
  });
});