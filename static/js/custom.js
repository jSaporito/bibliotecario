// Custom JavaScript for Bibliotecario Flask Application

// Global variables
let progressPollingInterval;
let processingStartTime;

// Document ready
$(document).ready(function() {
    initializeApplication();
});

// Initialize the application
function initializeApplication() {
    initializeTooltips();
    initializeFileUpload();
    initializeProgressBars();
    initializeFormValidation();
    initializeDownloadCards();
    setupErrorHandling();
    
    // Add fade-in animation to cards
    $('.card').addClass('fade-in');
    
    console.log('Bibliotecario application initialized');
}

// Initialize Bootstrap tooltips
function initializeTooltips() {
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

// File upload enhancements
function initializeFileUpload() {
    const fileInput = $('#file');
    const uploadArea = $('.file-upload-area');
    
    if (fileInput.length) {
        // File input change event
        fileInput.on('change', function() {
            handleFileSelect(this.files[0]);
        });
        
        // Drag and drop functionality
        if (uploadArea.length) {
            uploadArea.on('dragover', function(e) {
                e.preventDefault();
                $(this).addClass('dragover');
            });
            
            uploadArea.on('dragleave', function(e) {
                e.preventDefault();
                $(this).removeClass('dragover');
            });
            
            uploadArea.on('drop', function(e) {
                e.preventDefault();
                $(this).removeClass('dragover');
                
                const files = e.originalEvent.dataTransfer.files;
                if (files.length > 0) {
                    handleFileSelect(files[0]);
                    fileInput[0].files = files;
                }
            });
        }
    }
}

// Handle file selection
function handleFileSelect(file) {
    if (!file) return;
    
    const maxSize = 100 * 1024 * 1024; // 100MB
    const allowedTypes = ['text/csv', 'application/vnd.ms-excel'];
    
    // Validate file size
    if (file.size > maxSize) {
        showAlert('File size exceeds 100MB limit. Please choose a smaller file.', 'danger');
        return false;
    }
    
    // Validate file type
    if (!file.name.toLowerCase().endsWith('.csv')) {
        showAlert('Please select a CSV file.', 'warning');
        return false;
    }
    
    // Show file info
    const fileSize = formatFileSize(file.size);
    const fileInfo = `
        <div class="file-info mt-2">
            <i class="fas fa-check-circle text-success me-2"></i>
            <strong>${file.name}</strong> (${fileSize})
        </div>
    `;
    
    $('.file-upload-area').html(fileInfo);
    
    return true;
}

// Progress polling function
function pollProgress(sessionId) {
    if (!sessionId) return;
    
    processingStartTime = Date.now();
    
    const progressBar = $('#progressBar');
    const statusMessage = $('#statusMessage');
    const progressContainer = $('#progressContainer');
    
    function updateProgress() {
        fetch(`/api/status/${sessionId}`)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                return response.json();
            })
            .then(data => {
                // Update progress bar
                if (progressBar.length) {
                    const progress = Math.min(100, Math.max(0, data.progress || 0));
                    progressBar.css('width', progress + '%')
                             .attr('aria-valuenow', progress)
                             .text(progress + '%');
                }
                
                // Update status message
                if (statusMessage.length) {
                    statusMessage.text(data.message || 'Processing...');
                }
                
                // Calculate elapsed time
                const elapsed = Math.floor((Date.now() - processingStartTime) / 1000);
                $('#elapsedTime').text(formatDuration(elapsed));
                
                // Handle completion
                if (data.status === 'completed') {
                    handleProcessingComplete(sessionId, progressContainer);
                } else if (data.status === 'error') {
                    handleProcessingError(data.message, progressContainer);
                } else {
                    // Continue polling
                    progressPollingInterval = setTimeout(updateProgress, 2000);
                }
            })
            .catch(error => {
                console.error('Error polling progress:', error);
                handlePollingError(error, progressContainer);
            });
    }
    
    updateProgress();
}

// Handle processing completion
function handleProcessingComplete(sessionId, progressContainer) {
    if (progressPollingInterval) {
        clearTimeout(progressPollingInterval);
    }
    
    if (progressContainer && progressContainer.length) {
        progressContainer.html(`
            <div class="alert alert-success slide-up">
                <i class="fas fa-check-circle me-2"></i>
                <strong>Processing completed successfully!</strong>
                <br><small>Redirecting to results page...</small>
            </div>
        `);
    }
    
    // Redirect to results page
    setTimeout(() => {
        window.location.href = `/results/${sessionId}`;
    }, 2000);
}

// Handle processing error
function handleProcessingError(message, progressContainer) {
    if (progressPollingInterval) {
        clearTimeout(progressPollingInterval);
    }
    
    if (progressContainer && progressContainer.length) {
        progressContainer.html(`
            <div class="alert alert-danger">
                <i class="fas fa-exclamation-triangle me-2"></i>
                <strong>Processing failed:</strong><br>
                ${message || 'An unknown error occurred'}
            </div>
        `);
    }
}

// Handle polling errors
function handlePollingError(error, progressContainer) {
    console.error('Polling error:', error);
    
    // Retry after 5 seconds
    progressPollingInterval = setTimeout(() => {
        // Try to resume polling
        const sessionId = window.location.pathname.split('/').pop();
        pollProgress(sessionId);
    }, 5000);
}

// Initialize progress bars with animation
function initializeProgressBars() {
    // Animate progress bars on results page
    $('.progress-bar[data-percentage]').each(function() {
        const $bar = $(this);
        const percentage = $bar.data('percentage') || 0;
        
        // Start from 0 and animate to target
        $bar.css('width', '0%');
        
        setTimeout(() => {
            $bar.css('width', percentage + '%');
        }, 300);
    });
    
    // Generic progress bar animation
    $('.progress-bar').each(function() {
        const $bar = $(this);
        const targetWidth = $bar.css('width');
        
        if (targetWidth && targetWidth !== '0px') {
            $bar.css('width', '0%');
            setTimeout(() => {
                $bar.css('width', targetWidth);
            }, 200);
        }
    });
}

// Form validation
function initializeFormValidation() {
    // Upload form validation
    $('#uploadForm').on('submit', function(e) {
        const fileInput = $('#file')[0];
        const obsColumn = $('#obs_column').val();
        const chunkSize = parseInt($('#chunk_size').val());
        
        let hasErrors = false;
        
        // Validate file
        if (!fileInput || !fileInput.files || fileInput.files.length === 0) {
            showAlert('Please select a CSV file to upload.', 'danger');
            hasErrors = true;
        }
        
        // Validate obs column
        if (!obsColumn || obsColumn.trim() === '') {
            showAlert('Please specify the text column name.', 'danger');
            hasErrors = true;
        }
        
        // Validate chunk size
        if (isNaN(chunkSize) || chunkSize < 100 || chunkSize > 10000) {
            showAlert('Chunk size must be between 100 and 10,000.', 'danger');
            hasErrors = true;
        }
        
        if (hasErrors) {
            e.preventDefault();
            return false;
        }
        
        // Show loading state
        const submitBtn = $('#submitBtn');
        submitBtn.prop('disabled', true)
                 .html('<i class="fas fa-spinner fa-spin me-2"></i>Uploading & Starting Process...');
        
        // Show loading alert
        const loadingAlert = `
            <div class="alert alert-info fade-in" role="alert">
                <i class="fas fa-upload me-2"></i>
                Uploading file and starting processing. Please wait...
                <div class="progress mt-2">
                    <div class="progress-bar progress-bar-striped progress-bar-animated" 
                         style="width: 100%"></div>
                </div>
            </div>
        `;
        $(this).before(loadingAlert);
    });
}

// Initialize download cards
function initializeDownloadCards() {
    $('.download-card').on('click', function() {
        const downloadUrl = $(this).find('a').attr('href');
        if (downloadUrl) {
            // Add downloading state
            $(this).addClass('downloading');
            
            // Remove state after 2 seconds
            setTimeout(() => {
                $(this).removeClass('downloading');
            }, 2000);
        }
    });
}

// Utility Functions
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDuration(seconds) {
    if (seconds < 60) {
        return seconds + 's';
    } else if (seconds < 3600) {
        const minutes = Math.floor(seconds / 60);
        const remainingSeconds = seconds % 60;
        return minutes + 'm ' + remainingSeconds + 's';
    } else {
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        return hours + 'h ' + minutes + 'm';
    }
}

function formatNumber(number) {
    return new Intl.NumberFormat().format(number);
}

// Alert system
function showAlert(message, type = 'info', duration = 5000) {
    const alertId = 'alert-' + Date.now();
    const alertClass = type === 'error' ? 'danger' : type;
    
    const alertHtml = `
        <div id="${alertId}" class="alert alert-${alertClass} alert-dismissible fade show" role="alert">
            <i class="fas fa-${getAlertIcon(type)} me-2"></i>
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    // Add to page
    if ($('.alert-container').length) {
        $('.alert-container').append(alertHtml);
    } else {
        $('main .container').prepend(alertHtml);
    }
    
    // Auto dismiss
    if (duration > 0) {
        setTimeout(() => {
            $(`#${alertId}`).fadeOut(() => {
                $(`#${alertId}`).remove();
            });
        }, duration);
    }
}

function getAlertIcon(type) {
    const icons = {
        'success': 'check-circle',
        'info': 'info-circle',
        'warning': 'exclamation-triangle',
        'danger': 'exclamation-circle',
        'error': 'exclamation-circle'
    };
    return icons[type] || 'info-circle';
}

// Copy to clipboard
function copyToClipboard(text) {
    if (navigator.clipboard) {
        navigator.clipboard.writeText(text).then(() => {
            showToast('Copied to clipboard!', 'success');
        }).catch(err => {
            console.error('Copy failed:', err);
            fallbackCopyToClipboard(text);
        });
    } else {
        fallbackCopyToClipboard(text);
    }
}

function fallbackCopyToClipboard(text) {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    
    try {
        document.execCommand('copy');
        showToast('Copied to clipboard!', 'success');
    } catch (err) {
        console.error('Fallback copy failed:', err);
        showToast('Copy failed', 'error');
    }
    
    document.body.removeChild(textArea);
}

// Toast notifications
function showToast(message, type = 'info', duration = 3000) {
    const toast = $(`
        <div class="toast-notification toast-${type}">
            <i class="fas fa-${getAlertIcon(type)} me-2"></i>
            ${message}
        </div>
    `);
    
    $('body').append(toast);
    
    setTimeout(() => {
        toast.fadeOut(() => toast.remove());
    }, duration);
}

// Error handling
function setupErrorHandling() {
    // Global error handler
    window.addEventListener('error', function(e) {
        console.error('Global error:', e.error);
        // Don't show user errors for minor issues
    });
    
    // Unhandled promise rejection handler
    window.addEventListener('unhandledrejection', function(e) {
        console.error('Unhandled promise rejection:', e.reason);
        e.preventDefault();
    });
}

// Page-specific functions
function previewData(sessionId) {
    $.get(`/api/preview/${sessionId}`)
        .done(function(data) {
            displayDataPreview(data);
        })
        .fail(function(xhr) {
            showAlert('Error loading data preview: ' + xhr.statusText, 'error');
        });
}

function displayDataPreview(data) {
    let html = '<div class="table-responsive"><table class="table table-sm table-striped">';
    html += '<thead><tr><th>Field</th><th>Count</th><th>Percentage</th><th>Sample Values</th></tr></thead><tbody>';
    
    data.fields.forEach(function(field) {
        html += `<tr>
            <td><strong>${field.field.replace(/_/g, ' ')}</strong></td>
            <td>${formatNumber(field.count)}</td>
            <td>${field.percentage}%</td>
            <td><small>${field.samples.slice(0, 3).join(', ')}</small></td>
        </tr>`;
    });
    
    html += '</tbody></table></div>';
    
    $('#previewContent').html(html);
    $('#previewModal').modal('show');
}

// Print functionality
function printPage() {
    window.print();
}

// Go back function
function goBack() {
    if (window.history.length > 1) {
        window.history.back();
    } else {
        window.location.href = '/';
    }
}

// Cleanup on page unload
$(window).on('beforeunload', function() {
    if (progressPollingInterval) {
        clearTimeout(progressPollingInterval);
    }
});

// Export functions for global access
window.BibliotecarioApp = {
    pollProgress: pollProgress,
    showAlert: showAlert,
    showToast: showToast,
    copyToClipboard: copyToClipboard,
    formatFileSize: formatFileSize,
    formatDuration: formatDuration,
    formatNumber: formatNumber,
    previewData: previewData,
    printPage: printPage,
    goBack: goBack
};