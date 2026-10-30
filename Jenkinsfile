// Scripted Pipeline - gives direct control over workspace allocation
properties([
    disableConcurrentBuilds(),
    pipelineTriggers([
        cron('0 2 1-7,15-21 * 0')  // Run bi-weekly on 1st and 3rd Sunday at 2 AM EST
    ]),
    parameters([
        string(name: 'SOURCE', defaultValue: 'all',
               description: "Source to process ('all' = run every source in translator_kg; otherwise a single source name)"),
        booleanParam(name: 'OVERWRITE', defaultValue: false, description: 'Overwrite existing files'),
        string(name: 'RELEASE_TAG', defaultValue: '',
               description: "Release tag (vX.Y.Z) of the code to build. Leave empty to build the latest release tag.")
    ])
])

def sources = []

// Force specific workspace path
node('transltr-ci-build-node-03-24.04') {
    dir('/home/deploy/jenkins/workspace/ci/KGX/kgx-ci-pipeline') {
        try {
            env.S3_BUCKET_NAME = 'kgx-translator-ingests'

            // Persistent cache on the mounted EBS volume. These directories are not
            // created automatically, so an unmounted volume fails the build immediately
            env.INGESTS_DATA_PATH = '/data/kgx-pipeline-cache/data'
            env.INGESTS_RELEASES_PATH = '/data/kgx-pipeline-cache/releases'


            stage('Checkout') {
                checkout scm

                // Enforce that we always checkout and run from a specific release tag.
                // Either use the one supplied by the RELEASE_TAG param, verifying it exists,
                // or find the tag for the most recent release from git and use that.
                def releaseTag = params.RELEASE_TAG?.trim() ?: sh(
                    returnStdout: true,
                    script: "git tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-v:refname | head -n 1"
                ).trim()
                if (!releaseTag) {
                    error("No release tags (vX.Y.Z) were found to build from")
                }
                if (!(releaseTag ==~ /v\d+\.\d+\.\d+/)) {
                    error("RELEASE_TAG ${releaseTag} is not a release tag of the form vX.Y.Z")
                }
                def tagExists = sh(returnStatus: true,
                                   script: "git rev-parse --verify --quiet 'refs/tags/${releaseTag}^{commit}'") == 0
                if (!tagExists) {
                    error("Release tag ${releaseTag} does not exist")
                }
                sh "git checkout --detach 'refs/tags/${releaseTag}'"
                echo "Building release ${releaseTag}"
            }

            stage('Install Dependencies') {
                sh 'uv sync'
            }

            stage('Verify Clean Checkout') {
                sh '''
                    if ! git diff --quiet HEAD; then
                        echo "ERROR: tracked files have uncommitted changes:"
                        git status --short --untracked-files=no
                        exit 1
                    fi
                    echo "Building code version: $(git describe --tags --match='v[0-9]*.[0-9]*.[0-9]*' --always)"
                '''
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

            stage('Create Releases') {
                echo "Creating release packages..."
                sh "make release SOURCES='${sources.join(' ')}'"
            }

            stage('Merge Sources') {
                def overwriteFlag = params.OVERWRITE ? 'OVERWRITE=true' : ''
                echo "Merging and releasing all multisource KGs"
                sh "make merge-all ${overwriteFlag}"
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