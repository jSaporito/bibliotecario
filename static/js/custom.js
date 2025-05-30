function pollProgress(sessionId) {
    const interval = setInterval(function() {
        $.get(`/api/status/${sessionId}`)
            .done(function(data) {
                // Update progress bar
                const progress = data.progress || 0;
                $('#progressBar').css('width', progress + '%')
                              .attr('aria-valuenow', progress)
                              .text(progress + '%');
                
                // Update status message
                $('#statusMessage').text(data.message || 'Processando...');
                
                // Check if completed
                if (data.status === 'completed') {
                    clearInterval(interval);
                    
                    // Update UI for completion
                    $('#progressBar').removeClass('progress-bar-striped progress-bar-animated')
                                   .addClass('bg-success');
                    $('#statusMessage').html('<i class="fas fa-check-circle text-success me-2"></i>Processamento concluído com sucesso!');
                    
                    // Redirect to results page
                    setTimeout(function() {
                        window.location.href = `/results/${sessionId}`;
                    }, 2000);
                    
                } else if (data.status === 'error') {
                    clearInterval(interval);
                    
                    // Update UI for error
                    $('#progressBar').removeClass('progress-bar-striped progress-bar-animated')
                                   .addClass('bg-danger');
                    $('#statusMessage').html(`<i class="fas fa-exclamation-triangle text-danger me-2"></i>${data.message}`);
                    
                    // Show error alert
                    const errorAlert = `
                        <div class="alert alert-danger mt-3" role="alert">
                            <h6><i class="fas fa-exclamation-triangle me-2"></i>Processamento Falhou</h6>
                            <p class="mb-0">${data.message}</p>
                            <hr>
                            <a href="/upload" class="btn btn-outline-danger btn-sm">
                                <i class="fas fa-redo me-1"></i>Tente Novamente
                            </a>
                        </div>
                    `;
                    $('#progressContainer').after(errorAlert);
                }
            })
            .fail(function() {
                clearInterval(interval);
                $('#statusMessage').html('<i class="fas fa-exclamation-triangle text-warning me-2"></i>Erro de conexão. Atualize a página.');
            });
    }, 2000); // Poll every 2 seconds
}

// Initialize page-specific functionality
$(document).ready(function() {
    // Add smooth scrolling to anchor links
    $('a[href^="#"]').on('click', function(event) {
        const target = $(this.getAttribute('href'));
        if (target.length) {
            event.preventDefault();
            $('html, body').stop().animate({
                scrollTop: target.offset().top - 100
            }, 1000);
        }
    });
    
    // Auto-hide flash messages after 5 seconds
    setTimeout(function() {
        $('.alert-dismissible').fadeOut('slow');
    }, 5000);
    
    // File upload preview
    $('input[type="file"]').on('change', function() {
        const fileName = $(this).val().split('\\').pop();
        if (fileName) {
            $(this).next('.form-text').text(`Selecionado: ${fileName}`);
        }
    });
    
    // Add loading state to buttons on form submit
    $('form').on('submit', function() {
        $(this).find('button[type="submit"]').prop('disabled', true)
               .html('<i class="fas fa-spinner fa-spin me-2"></i>Processando...');
    });
});

// Utility functions
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatNumber(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Export functions for global use
window.pollProgress = pollProgress;
window.formatFileSize = formatFileSize;
window.formatNumber = formatNumber;