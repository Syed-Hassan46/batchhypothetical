namespace BatchDotnetFfmpeg


{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Batch;
    using Microsoft.Azure.Batch.Auth;
    using Microsoft.Azure.Batch.Common;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    public class Program
    {
        
        // Batch account credentials
        private const string BatchAccountName = "ivdo";
        private const string BatchAccountKey = "ak+Zcub/WuRUkt3ujcSs0reV/o2qHZwEjeo4PgdqxswVDcFYXGLxfRhJIyL18OR/2zrqzyKBK9XyJwAmn7DN5A==";
        private const string BatchAccountUrl = "https://ivdo.eastus.batch.azure.com";


        // Storage account credentials
        private const string StorageAccountName = "ivdobatch";
        private const string StorageAccountKey = "+pPxTDfuKTjMBwoPjnmBa2LK3BkTq6lbZXaAPUBK+YMD6b8YAoOtubl91vJa0PPJ9d0CRv9Mmv863WP0YBO5yQ==";

        // Pool and Job constants
        private const string PoolId = "IvdoWinFFmpegPool";
        private const int DedicatedNodeCount = 0;
        private const int LowPriorityNodeCount = 5;
        private const string PoolVMSize = "STANDARD_A1_v2";
        private const string JobId = "IvdoWinFFmpegJob";

        
        const string appPackageId = "ffmpeg";
        const string appPackageVersion = "4.3.1";

        public static void Main(string[] args)
        {
            if (String.IsNullOrEmpty(BatchAccountName) ||
                String.IsNullOrEmpty(BatchAccountKey) ||
                String.IsNullOrEmpty(BatchAccountUrl) ||
                String.IsNullOrEmpty(StorageAccountName) ||
                String.IsNullOrEmpty(StorageAccountKey))
            {
                throw new InvalidOperationException("One or more account credential strings have not been populated. Please ensure that your Batch and Storage account credentials have been specified.");
            }

            try
            {
                MainAsync().Wait();
            }
            catch (AggregateException)
            {
                Console.WriteLine();
                Console.WriteLine("One or more exceptions occurred.");
                Console.WriteLine();
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("Sample complete, hit ENTER to exit...");
                Console.ReadLine();
            }
        }

        /// <summary>
        /// </summary>
        /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
        private static async Task MainAsync()
        {
            Console.WriteLine("Sample start: {0}", DateTime.Now);
            Console.WriteLine();
            Stopwatch timer = new Stopwatch();
            timer.Start();

            // Construct the Storage account connection string
            string storageConnectionString = String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                                StorageAccountName, StorageAccountKey);

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

             
            // Use the blob client to create the containers in blob storage
            const string inputContainerName = "input";
            const string outputContainerName = "output";

            await CreateContainerIfNotExistAsync(blobClient, inputContainerName);
            await CreateContainerIfNotExistAsync(blobClient, outputContainerName);

            
            string inputPath = Path.Combine(Environment.CurrentDirectory, "InputFiles");

            List<string> inputFilePaths = new List<string>(Directory.GetFileSystemEntries(inputPath, "*.mp4",
                                         SearchOption.TopDirectoryOnly));
               
            List<ResourceFile> inputFiles = await UploadFilesToContainerAsync(blobClient, inputContainerName, inputFilePaths);

            string outputContainerSasUrl = GetContainerSasUrl(blobClient, outputContainerName, SharedAccessBlobPermissions.Write);


            BatchSharedKeyCredentials sharedKeyCredentials = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);

            using (BatchClient batchClient = BatchClient.Open(sharedKeyCredentials))
            {
                await CreatePoolIfNotExistAsync(batchClient, PoolId);

                // Create the job that runs the tasks.
                await CreateJobAsync(batchClient, JobId, PoolId);

                // Create a collection of tasks and add them to the Batch job. 
                await AddTasksAsync(batchClient, JobId, inputFiles, outputContainerSasUrl);

                // Monitor task success or failure, specifying a maximum amount of time to wait for
                await MonitorTasks(batchClient, JobId, TimeSpan.FromMinutes(60));

                // Delete input container in storage
                Console.WriteLine("Deleting container [{0}]...", inputContainerName);
                CloudBlobContainer container = blobClient.GetContainerReference(inputContainerName);
                await container.DeleteIfExistsAsync();
                   
                // Print out timing info
                timer.Stop();
                Console.WriteLine();
                Console.WriteLine("Sample end: {0}", DateTime.Now);
                Console.WriteLine("Elapsed time: {0}", timer.Elapsed);

                Console.WriteLine();
                Console.Write("Deleting job...");    
                await batchClient.JobOperations.DeleteJobAsync(JobId);
                

                Console.Write("Deleting pool...");
                await batchClient.PoolOperations.DeletePoolAsync(PoolId);
                
            }
        }
       
        /// <summary>
        /// </summary>
        /// <param name="blobClient">A <see cref="CloudBlobClient"/>.</param>
        /// <param name="containerName">The name for the new container.</param>

        private static async Task CreateContainerIfNotExistAsync(CloudBlobClient blobClient, string containerName)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();
            Console.WriteLine("Creating container [{0}].", containerName);
        }


        // RESOURCE FILE SETUP - FUNCTION IMPLEMENTATIONS

        /// <summary>
        /// </summary>
        /// <param name="blobClient">A <see cref="CloudBlobClient"/>.</param>
        /// <param name="containerName">Name of the blob storage container to which the files are uploaded.</param>
        /// <param name="filePaths">A collection of paths of the files to be uploaded to the container.</param>
        /// <returns>A collection of <see cref="ResourceFile"/> objects.</returns>
        private static async Task<List<ResourceFile>> UploadFilesToContainerAsync(CloudBlobClient blobClient, string inputContainerName, List<string> filePaths)
        {
            List<ResourceFile> resourceFiles = new List<ResourceFile>();

            foreach (string filePath in filePaths)
            {
                resourceFiles.Add(await UploadResourceFileToContainerAsync(blobClient, inputContainerName, filePath));
            }

            return resourceFiles;
        }

        /// <summary>
        /// </summary>
        /// <param name="blobClient">A <see cref="CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the blob storage container to which the file should be uploaded.</param>
        /// <param name="filePath">The full path to the file to upload to Storage.</param>
        /// <returns>A ResourceFile object representing the file in blob storage.</returns>
        private static async Task<ResourceFile> UploadResourceFileToContainerAsync(CloudBlobClient blobClient, string containerName, string filePath)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);

            string blobName = Path.GetFileName(filePath);
            var fileStream = System.IO.File.OpenRead(filePath);

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
            await blobData.UploadFromFileAsync(filePath);

            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
            };

            // Construct the SAS URL for blob
            string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
            string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);

            return new ResourceFile(blobSasUri, blobName);
        }

        /// <summary>
        /// </summary>
        /// <param name="blobClient">A <see cref="CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the container for which a SAS URL will be obtained.</param>
        /// <param name="permissions">The permissions granted by the SAS URL.</param>
        /// <returns>A SAS URL providing the specified access to the container.</returns>
        private static string GetContainerSasUrl(CloudBlobClient blobClient, string containerName, SharedAccessBlobPermissions permissions)
        {
            // Set the expiry time and permissions for the container access signature. In this case, no start time is specified,
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = permissions
            };

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            string sasContainerToken = container.GetSharedAccessSignature(sasConstraints);

            // Return the URL string for the container, including the SAS token
            return String.Format("{0}{1}", container.Uri, sasContainerToken);
        }


        // BATCH CLIENT OPERATIONS - FUNCTION IMPLEMENTATIONS

        /// <summary>
        /// Creates the Batch pool.
        /// </summary>
        /// <param name="batchClient">A BatchClient object</param>
        /// <param name="poolId">ID of the CloudPool object to create.</param>
        private static async Task CreatePoolIfNotExistAsync(BatchClient batchClient, string poolId)
        {
            CloudPool pool = null;
            try
            {
                Console.WriteLine("Creating pool [{0}]...", poolId);

                ImageReference imageReference = new ImageReference(
                        publisher: "MicrosoftWindowsServer",
                        offer: "WindowsServer",
                        sku: "2012-R2-Datacenter-smalldisk",
                        version: "latest");

                VirtualMachineConfiguration virtualMachineConfiguration =
                new VirtualMachineConfiguration(
                    imageReference: imageReference,
                    nodeAgentSkuId: "batch.node.windows amd64");

                pool = batchClient.PoolOperations.CreatePool(
                    poolId: poolId,
                    targetDedicatedComputeNodes: DedicatedNodeCount,
                    targetLowPriorityComputeNodes: LowPriorityNodeCount,
                    virtualMachineSize: PoolVMSize,                                                
                    virtualMachineConfiguration: virtualMachineConfiguration);  

                pool.ApplicationPackageReferences = new List<ApplicationPackageReference>
                {
                    new ApplicationPackageReference
                    {
                        ApplicationId = appPackageId,
                        Version = appPackageVersion
                    }
                };

                await pool.CommitAsync();
            }
            catch (BatchException be)
            {
                if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.PoolExists)
                {
                    Console.WriteLine("The pool {0} already existed when we tried to create it", poolId);
                }
                else
                {
                    throw; 
                }
            }
        }

        /// <summary>
        /// Creates a job in the specified pool.
        /// </summary>
        /// <param name="batchClient">A BatchClient object.</param>
        /// <param name="jobId">ID of the job to create.</param>
        /// <param name="poolId">ID of the CloudPool object in which to create the job.</param>
        private static async Task CreateJobAsync(BatchClient batchClient, string jobId, string poolId)
        {
            Console.WriteLine("Creating job [{0}]...", jobId);

            CloudJob job = batchClient.JobOperations.CreateJob();
            job.Id = jobId;
            job.PoolInformation = new PoolInformation { PoolId = poolId };

            await job.CommitAsync();
        }
       

        /// <summary>
        /// </summary>
        /// <param name="batchClient">A BatchClient object.</param>
        /// <param name="jobId">ID of the job to which the tasks are added.</param>
        /// <param name="inputFiles">A collection of ResourceFile objects representing the input file
        /// to be processed by the tasks executed on the compute nodes.</param>
        /// <param name="outputContainerSasUrl">The shared access signature URL for the Azure 
        /// <returns>A collection of the submitted cloud tasks.</returns>
        private static async Task<List<CloudTask>> AddTasksAsync(BatchClient batchClient, string jobId, List<ResourceFile> inputFiles, string outputContainerSasUrl)
        {
            Console.WriteLine("Adding {0} tasks to job [{1}]...", inputFiles.Count, jobId);

            List<CloudTask> tasks = new List<CloudTask>();

            for (int i = 0; i < inputFiles.Count; i++)
            {
                // Assign a task ID for each iteration
                string taskId = String.Format("Task{0}", i);

                string appPath = String.Format("%AZ_BATCH_APP_PACKAGE_{0}#{1}%", appPackageId, appPackageVersion);
                string inputMediaFile = inputFiles[i].FilePath;
                string outputMediaFile = String.Format("{0}{1}",
                    System.IO.Path.GetFileNameWithoutExtension(inputMediaFile),
                    ".avi");
                string taskCommandLine = String.Format("cmd /c {0}\\ffmpeg-4.3.1-2020-09-21-full_build\\bin\\ffmpeg.exe -i {1} {2}", appPath, inputMediaFile, outputMediaFile);

                CloudTask task = new CloudTask(taskId, taskCommandLine);
                task.ResourceFiles = new List<ResourceFile> { inputFiles[i] };

                
                List<OutputFile> outputFileList = new List<OutputFile>();
                OutputFileBlobContainerDestination outputContainer = new OutputFileBlobContainerDestination(outputContainerSasUrl);
                OutputFile outputFile = new OutputFile(outputMediaFile,
                                                       new OutputFileDestination(outputContainer),
                                                       new OutputFileUploadOptions(OutputFileUploadCondition.TaskSuccess));
                outputFileList.Add(outputFile);
                task.OutputFiles = outputFileList;
                tasks.Add(task);
            }

            await batchClient.JobOperations.AddTaskAsync(jobId, tasks);

            return tasks;
        }

        /// <summary>
        /// </summary>
        /// <param name="batchClient">A BatchClient object.</param>
        /// <param name="jobId">ID of the job containing the tasks to be monitored.</param>
        /// <param name="timeout">The period of time to wait for the tasks to reach the completed state.</param>
        private static async Task<bool> MonitorTasks(BatchClient batchClient, string jobId, TimeSpan timeout)
        {
            bool allTasksSuccessful = true;
            const string completeMessage = "All tasks reached state Completed.";
            const string incompleteMessage = "One or more tasks failed to reach the Completed state within the timeout period.";
            const string successMessage = "Success! All tasks completed successfully. Output files uploaded to output container.";
            const string failureMessage = "One or more tasks failed.";

        
            ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id");

            List<CloudTask> addedTasks = await batchClient.JobOperations.ListTasks(jobId, detail).ToListAsync();

            Console.WriteLine("Monitoring all tasks for 'Completed' state, timeout in {0}...", timeout.ToString());

        
            TaskStateMonitor taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();
            try
            {
                await taskStateMonitor.WhenAll(addedTasks, TaskState.Completed, timeout);
            }
            catch (TimeoutException)
            {
                await batchClient.JobOperations.TerminateJobAsync(jobId);
                Console.WriteLine(incompleteMessage);
                return false;
            }
            await batchClient.JobOperations.TerminateJobAsync(jobId);
            Console.WriteLine(completeMessage);

            detail.SelectClause = "executionInfo";
            detail.FilterClause = "executionInfo/result eq 'Failure'";

            List<CloudTask> failedTasks = await batchClient.JobOperations.ListTasks(jobId, detail).ToListAsync();
          
            if (failedTasks.Any())
            {
                allTasksSuccessful = false;
                Console.WriteLine(failureMessage);
            }
            else
            {
                Console.WriteLine(successMessage);
            }

            return allTasksSuccessful;
        }
    }
}
