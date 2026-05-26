sources = []

pipeline {
    agent { label 'transltr-ci-build-node-03-24.04' }

    options {
        // Disable concurrent builds to ensure workspace reuse
        // This prevents @2, @3 workspace directories and ensures all builds
        // use the main workspace where cached data (data/, releases/) lives
        disableConcurrentBuilds()
    }

    triggers {
        // Run bi-weekly (every 2 weeks) on 1st and 3rd Sunday at 2 AM EST
        cron('0 2 1-7,15-21 * 0')
    }

    parameters {
        string(name: 'SOURCE',
            defaultValue: 'all',
            description: "Source to process ('all' = run every source in translator_kg; otherwise a single source name)")
        booleanParam(name: 'OVERWRITE', defaultValue: false, description: 'Overwrite existing files')
    }

    environment {
        S3_BUCKET_NAME = 'kgx-translator-ingests'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Install Dependencies') {
            steps {
                sh 'uv sync'
            }
        }

        stage('Run Pipeline') {
            steps {
                script {
                    def overwriteFlag = params.OVERWRITE ? 'OVERWRITE=true' : ''

                    // Store results at pipeline level so other stages can access
                    env.PIPELINE_RESULTS = ''

                    if (params.SOURCE == 'all') {
                        // If "all" sources requested retrieve the complete list of sources
                        def sourcesLine = sh(
                            returnStdout: true,
                            script: 'uv run python -m translator_ingest.graphs all-sources'
                        ).trim()
                        // Split the list into an array
                        sources = sourcesLine.split(/\s+/)
                    } else {
                        // Otherwise set sources to the one source requested
                        sources = [params.SOURCE]
                    }

                    // Run each source - pipeline will internally skip steps that don't need updating
                    def results = [:]
                    for (source in sources) {
                        try {
                            echo "Processing ${source}..."
                            sh "make run SOURCES=${source} ${overwriteFlag}"
                            results[source] = 'SUCCESS'
                        } catch (Exception e) {
                            echo "ERROR: ${source} failed: ${e.message}"
                            results[source] = 'FAILED'
                            // Continue to next source instead of failing the build
                        }
                    }

                    // Report results
                    echo "\n=== Pipeline Results ==="
                    results.each { source, status ->
                        echo "${source}: ${status}"
                    }

                    // Log warning if any source failed, but continue to merge/release
                    def failedSources = results.findAll { it.value == 'FAILED' }.keySet()
                    def succeededCount = results.size() - failedSources.size()
                    if (failedSources) {
                        currentBuild.result = 'UNSTABLE'
                        echo "\nWARNING: ${failedSources.size()} source(s) failed: ${failedSources}"
                        echo "Continuing with merge/release for successful sources..."
                    }

                    // Surface per-source results on the build summary page
                    currentBuild.description = failedSources ?
                        "${succeededCount}/${results.size()} ok — failed: ${failedSources.join(', ')}" :
                        "${results.size()}/${results.size()} ok"

                    // Store results for next stage
                    env.PIPELINE_RESULTS = results.collect { k, v -> "${k}:${v}" }.join(',')

                }
            }
        }

        stage('Merge Sources') {
            steps {
                script {
                    def overwriteFlag = params.OVERWRITE ? 'OVERWRITE=true' : ''

                    // Run merge-all to merge sources into multisource KGs ie translator_kg
                    echo "Merging and releasing all multisource KGs"
                    sh "make merge-all ${overwriteFlag}"
                }
            }
        }

        stage('Create Releases') {
            steps {
                script {
                    echo "Creating release packages..."
                    sh "make release SOURCES='${sources.join(' ')}'"
                }
            }
        }

        stage('Upload to S3') {
            steps {
                script {
                    echo "Uploading all data and releases to S3..."
                    sh "make upload-all"
                }
            }
        }
    }
}