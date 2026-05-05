pipeline {
    agent { label 'transltr-ci-build-node-03-24.04' }
    
    triggers {
        // Run bi-weekly (every 2 weeks) on 1st and 3rd Sunday at 2 AM EST
        cron('0 2 1-7,15-21 * 0')
    }
    
    parameters {
        choice(name: 'SOURCE', 
            choices: [
                'all',
                'alliance',
                'bgee',
                'bindingdb',
                'chembl',
                'cohd',
                'ctd',
                'ctkp',
                'dakp',
                'dgidb',
                'diseases',
                'drug_rep_hub',
                'drugcentral',
                'gene2phenotype',
                'geneticskp',
                'go_cam',
                'goa',
                'gtopdb',
                'hpoa',
                'icees',
                'intact',
                'ncbi_gene',
                'panther',
                'pathbank',
                'semmeddb',
                'sider',
                'signor',
                'tmkp',
                'ttd',
                'ubergraph'
            ], 
            description: 'Source to process (all = run all sources)')
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
        
        stage('Run Pipeline and Upload') {
            steps {
                script {
                    def overwriteFlag = params.OVERWRITE ? 'OVERWRITE=true' : ''
                    
                    // Store results at pipeline level so other stages can access
                    env.PIPELINE_RESULTS = ''
                    
                    if (params.SOURCE == 'all') {
                        // Get list of all sources from Makefile
                        def sources = ['alliance', 'bgee', 'bindingdb', 'chembl', 'cohd', 'ctd', 
                                      'ctkp', 'dakp', 'dgidb', 'diseases', 'drug_rep_hub', 
                                      'drugcentral', 'gene2phenotype', 'geneticskp', 'go_cam', 
                                      'goa', 'gtopdb', 'hpoa', 'icees', 'intact', 'ncbi_gene', 
                                      'panther', 'pathbank', 'semmeddb', 'sider', 'signor', 
                                      'tmkp', 'ttd', 'ubergraph']
                        
                        // Run each source that needs updating
                        def results = [:]
                        for (source in sources) {
                            try {
                                // Check if source needs update (unless OVERWRITE is set)
                                def needsUpdate = true
                                if (!params.OVERWRITE) {
                                    def checkResult = sh(
                                        script: "uv run python check_source_needs_update.py ${source}",
                                        returnStatus: true
                                    )
                                    needsUpdate = (checkResult == 0)
                                    
                                    if (!needsUpdate) {
                                        echo "Skipping ${source} - already up to date"
                                        results[source] = 'SKIPPED'
                                        continue
                                    }
                                }
                                
                                echo "Processing ${source}..."
                                sh "make run SOURCES=${source} ${overwriteFlag}"
                                sh "make upload SOURCES=${source}"
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
                        def failedSources = results.findAll { it.value == 'FAILED' }
                        if (failedSources) {
                            echo "\nWARNING: ${failedSources.size()} source(s) failed: ${failedSources.keySet()}"
                            echo "Continuing with merge/release for successful sources..."
                        }
                        
                        // Store results for next stage
                        env.PIPELINE_RESULTS = results.collect { k, v -> "${k}:${v}" }.join(',')
                    } else {
                        // Run and upload specific source
                        // Check if source needs update (unless OVERWRITE is set)
                        def needsUpdate = true
                        if (!params.OVERWRITE) {
                            def checkResult = sh(
                                script: "uv run python check_source_needs_update.py ${params.SOURCE}",
                                returnStatus: true
                            )
                            needsUpdate = (checkResult == 0)
                            
                            if (!needsUpdate) {
                                echo "Source ${params.SOURCE} is already up to date. Skipping."
                                echo "Use OVERWRITE=true to force reprocessing."
                                return
                            }
                        }
                        
                        echo "Processing ${params.SOURCE}..."
                        sh "make run SOURCES=${params.SOURCE} ${overwriteFlag}"
                    }
                }
            }
        }
        
        stage('Download Skipped Sources from S3') {
            when {
                expression { params.SOURCE == 'all' && !params.OVERWRITE }
            }
            steps {
                script {
                    // Parse results from previous stage
                    if (env.PIPELINE_RESULTS) {
                        def results = [:]
                        env.PIPELINE_RESULTS.split(',').each { entry ->
                            def parts = entry.split(':')
                            results[parts[0]] = parts[1]
                        }
                        
                        def skippedSources = results.findAll { it.value == 'SKIPPED' }.keySet()
                        def downloadFailed = []
                        
                        if (skippedSources) {
                            echo "Downloading ${skippedSources.size()} skipped source(s) from S3 for merge: ${skippedSources}"
                            
                            skippedSources.each { source ->
                                try {
                                    echo "Downloading ${source} from S3..."
                                    def downloadResult = sh(
                                        script: """
                                            mkdir -p data/${source}
                                            aws s3 sync s3://${env.S3_BUCKET_NAME}/data/${source}/ data/${source}/ --exclude "*.tar.gz"
                                            # Check if any files were downloaded
                                            if [ -z "\$(ls -A data/${source})" ]; then
                                                echo "ERROR: No files downloaded for ${source}"
                                                exit 1
                                            fi
                                        """,
                                        returnStatus: true
                                    )
                                    if (downloadResult != 0) {
                                        downloadFailed.add(source)
                                        echo "WARNING: Failed to download ${source} from S3 - will exclude from merge"
                                    }
                                } catch (Exception e) {
                                    downloadFailed.add(source)
                                    echo "WARNING: Failed to download ${source} from S3: ${e.message} - will exclude from merge"
                                }
                            }
                            
                            // Update PIPELINE_RESULTS to mark download failures as FAILED
                            if (downloadFailed) {
                                downloadFailed.each { source ->
                                    results[source] = 'FAILED'
                                }
                                env.PIPELINE_RESULTS = results.collect { k, v -> "${k}:${v}" }.join(',')
                            }
                        } else {
                            echo "No skipped sources to download from S3"
                        }
                    }
                }
            }
        }
        
        stage('Merge Sources') {
            when {
                expression { params.SOURCE == 'all' }
            }
            steps {
                script {
                    def overwriteFlag = params.OVERWRITE ? 'OVERWRITE=true' : ''
                    
                    // Parse results to get successful and skipped sources only (exclude failed)
                    def sourcesToMerge = []
                    if (env.PIPELINE_RESULTS) {
                        def results = [:]
                        env.PIPELINE_RESULTS.split(',').each { entry ->
                            def parts = entry.split(':')
                            results[parts[0]] = parts[1]
                        }
                        
                        // Only merge sources that succeeded or were skipped
                        sourcesToMerge = results.findAll { it.value in ['SUCCESS', 'SKIPPED'] }.keySet().join(' ')
                        
                        def failedSources = results.findAll { it.value == 'FAILED' }.keySet()
                        if (failedSources) {
                            echo "NOTE: Excluding failed source(s) from merge: ${failedSources}"
                        }
                    } else {
                        // Fallback: merge all sources
                        sourcesToMerge = 'alliance bgee bindingdb chembl cohd ctd ctkp dakp dgidb diseases drug_rep_hub drugcentral gene2phenotype geneticskp go_cam goa gtopdb hpoa icees intact ncbi_gene panther pathbank semmeddb sider signor tmkp ttd ubergraph'
                    }
                    
                    echo "Merging sources into translator_kg: ${sourcesToMerge}"
                    sh "make merge-all SOURCES='${sourcesToMerge}' ${overwriteFlag}"
                }
            }
        }
        
        stage('Create Releases') {
            when {
                expression { params.SOURCE == 'all' }
            }
            steps {
                script {
                    echo "Creating release packages..."
                    sh "make release"
                }
            }
        }
        
        stage('Upload to S3') {
            steps {
                script {
                    if (params.SOURCE == 'all') {
                        echo "Uploading all data and releases to S3..."
                        sh "make upload-all"
                    } else {
                        echo "Uploading ${params.SOURCE} to S3..."
                        sh "make upload SOURCES=${params.SOURCE}"
                    }
                }
            }
        }
    }
}