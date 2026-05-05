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
                        
                        // Run each source - pipeline will internally skip steps that don't need updating
                        def results = [:]
                        for (source in sources) {
                            try {
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
                        echo "Processing ${params.SOURCE}..."
                        sh "make run SOURCES=${params.SOURCE} ${overwriteFlag}"
                        sh "make upload SOURCES=${params.SOURCE}"
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
                    
                    // Parse results to get successful sources only (exclude failed)
                    def sourcesToMerge = []
                    if (env.PIPELINE_RESULTS) {
                        def results = [:]
                        env.PIPELINE_RESULTS.split(',').each { entry ->
                            def parts = entry.split(':')
                            results[parts[0]] = parts[1]
                        }
                        
                        // Only merge sources that succeeded
                        sourcesToMerge = results.findAll { it.value == 'SUCCESS' }.keySet().join(' ')
                        
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