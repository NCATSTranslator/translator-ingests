// Scripted Pipeline - gives direct control over workspace allocation
properties([
    disableConcurrentBuilds(),
    pipelineTriggers([
        cron('0 2 1-7,15-21 * 0')  // Run bi-weekly on 1st and 3rd Sunday at 2 AM EST
    ]),
    parameters([
        string(name: 'SOURCE', defaultValue: 'all', 
               description: "Source to process ('all' = run every source in translator_kg; otherwise a single source name)"),
        booleanParam(name: 'OVERWRITE', defaultValue: false, description: 'Overwrite existing files')
    ])
])

def sources = []

// Force specific workspace path
node('transltr-ci-build-node-03-24.04') {
    dir('/home/deploy/jenkins/workspace/ci/KGX/kgx-ci-pipeline') {
        try {
            env.S3_BUCKET_NAME = 'kgx-translator-ingests'
            
            stage('Checkout') {
                checkout scm
            }
            
            stage('Install Dependencies') {
                sh 'uv sync'
            }
            
            stage('Run Pipeline') {
                def overwriteFlag = params.OVERWRITE ? 'OVERWRITE=true' : ''
                
                // Get list of sources
                if (params.SOURCE == 'all') {
                    def sourcesLine = sh(
                        returnStdout: true,
                        script: 'uv run python -m translator_ingest.graphs all-sources'
                    ).trim()
                    sources = sourcesLine.split(/\s+/)
                } else {
                    sources = [params.SOURCE]
                }
                
                // Run each source
                def results = [:]
                for (source in sources) {
                    try {
                        echo "Processing ${source}..."
                        sh "make run SOURCES=${source} ${overwriteFlag}"
                        results[source] = 'SUCCESS'
                    } catch (Exception e) {
                        echo "ERROR: ${source} failed: ${e.message}"
                        results[source] = 'FAILED'
                    }
                }
                
                // Report results
                echo "\n=== Pipeline Results ==="
                results.each { source, status ->
                    echo "${source}: ${status}"
                }
                
                def failedSources = results.findAll { it.value == 'FAILED' }.keySet()
                def succeededCount = results.size() - failedSources.size()
                if (failedSources) {
                    currentBuild.result = 'UNSTABLE'
                    echo "\nWARNING: ${failedSources.size()} source(s) failed: ${failedSources}"
                    echo "Continuing with merge/release for successful sources..."
                }
                
                currentBuild.description = failedSources ?
                    "${succeededCount}/${results.size()} ok — failed: ${failedSources.join(', ')}" :
                    "${results.size()}/${results.size()} ok"
                
                env.PIPELINE_RESULTS = results.collect { k, v -> "${k}:${v}" }.join(',')
            }
            
            stage('Merge Sources') {
                def overwriteFlag = params.OVERWRITE ? 'OVERWRITE=true' : ''
                echo "Merging and releasing all multisource KGs"
                sh "make merge-all ${overwriteFlag}"
            }
            
            stage('Create Releases') {
                echo "Creating release packages..."
                sh "make release SOURCES='${sources.join(' ')}'"
            }
            
            stage('Upload to S3') {
                echo "Uploading all data and releases to S3..."
                sh "make upload-all"
            }
            
        } catch (Exception e) {
            currentBuild.result = 'FAILURE'
            throw e
        }
    }
}