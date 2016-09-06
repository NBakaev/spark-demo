//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package ru.nbakaev.spark;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.*;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.*;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3CompatibleClientForHadoop extends S3AFileSystem {
    public static final int DEFAULT_BLOCKSIZE = 33554432;
    private URI uri;
    private Path workingDir;
    private AmazonS3Client s3;
    private String bucket;
    private int maxKeys;
    private long partSize;
    private TransferManager transfers;
    private ThreadPoolExecutor threadPoolExecutor;
    private int multiPartThreshold;
    public static final Logger LOG = LoggerFactory.getLogger(S3CompatibleClientForHadoop.class);
    private CannedAccessControlList cannedACL;
    private String serverSideEncryptionAlgorithm;
    private static final int MAX_ENTRIES_TO_DELETE = 1000;
    private static final AtomicInteger poolNumber = new AtomicInteger(1);

    public static AmazonS3Client getS3Client(String accessKey, String secretKey, String endpoint){
            AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setProtocol(Protocol.HTTP);
            clientConfiguration.setSignerOverride("AWSS3V4SignerType");

            AmazonS3Client amazonS3Client = new AmazonS3Client(credentials, clientConfiguration);
            amazonS3Client.setEndpoint(endpoint);
            amazonS3Client.setS3ClientOptions(new S3ClientOptions());
//        amazonS3Client.setRegion( Region.getRegion(Regions.US_EAST_1) ); // error in minio

            S3ClientOptions s3ClientOptions = new S3ClientOptions();
            s3ClientOptions.setPathStyleAccess(true);
            s3ClientOptions.disableChunkedEncoding();

            amazonS3Client.setS3ClientOptions(s3ClientOptions);
            // TODO: not global disable
            // https://github.com/aws/aws-sdk-java/blob/b27a6a6e9bbfa55aed4f4176a5a97dd106444ba4/aws-java-sdk-s3/src/main/java/com/amazonaws/services/s3/internal/SkipMd5CheckStrategy.java
            System.setProperty("com.amazonaws.common.s3.disableGetObjectMD5Validation", "true");

        return amazonS3Client;
    }

    public static ThreadFactory getNamedThreadFactory(final String prefix) {
        SecurityManager s = System.getSecurityManager();
        final ThreadGroup threadGroup = s != null?s.getThreadGroup():Thread.currentThread().getThreadGroup();
        return new ThreadFactory() {
            final AtomicInteger threadNumber = new AtomicInteger(1);
            private final int poolNum;
            final ThreadGroup group;

            {
                this.poolNum = S3CompatibleClientForHadoop.poolNumber.getAndIncrement();
                this.group = threadGroup;
            }

            public Thread newThread(Runnable r) {
                String name = prefix + "-pool" + this.poolNum + "-t" + this.threadNumber.getAndIncrement();
                return new Thread(this.group, r, name);
            }
        };
    }

    private static ThreadFactory newDaemonThreadFactory(String prefix) {
        final ThreadFactory namedFactory = getNamedThreadFactory(prefix);
        return new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = namedFactory.newThread(r);
                if(!t.isDaemon()) {
                    t.setDaemon(true);
                }

                if(t.getPriority() != 5) {
                    t.setPriority(5);
                }

                return t;
            }
        };
    }

    public void initialize(URI name, Configuration conf) throws IOException {
        this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());
        this.workingDir = (new Path("/user", System.getProperty("user.name"))).makeQualified(this.uri, this.getWorkingDirectory());
        String accessKey = conf.get("fs.s3a.access.key", (String)null);
        String secretKey = conf.get("fs.s3a.secret.key", (String)null);
        String userInfo = name.getUserInfo();
        if(userInfo != null) {
            int credentials = userInfo.indexOf(58);
            if(credentials != -1) {
                accessKey = userInfo.substring(0, credentials);
                secretKey = userInfo.substring(credentials + 1);
            } else {
                accessKey = userInfo;
            }
        }

        AWSCredentialsProviderChain credentials1 = new AWSCredentialsProviderChain(new AWSCredentialsProvider[]{new BasicAWSCredentialsProvider(accessKey, secretKey), new InstanceProfileCredentialsProvider(), new AnonymousAWSCredentialsProvider()});
        this.bucket = name.getHost();
        ClientConfiguration awsConf = new ClientConfiguration();
        awsConf.setMaxConnections(conf.getInt("fs.s3a.connection.maximum", 15));
        boolean secureConnections = conf.getBoolean("fs.s3a.connection.ssl.enabled", true);
        awsConf.setProtocol(secureConnections?Protocol.HTTPS:Protocol.HTTP);
        awsConf.setMaxErrorRetry(conf.getInt("fs.s3a.attempts.maximum", 10));
        awsConf.setConnectionTimeout(conf.getInt("fs.s3a.connection.establish.timeout", 5000));
        awsConf.setSocketTimeout(conf.getInt("fs.s3a.connection.timeout", 50000));
        String proxyHost = conf.getTrimmed("fs.s3a.proxy.host", "");
        int proxyPort = conf.getInt("fs.s3a.proxy.port", -1);
        String endPoint = null;
        String coreThreads;
        if(!proxyHost.isEmpty()) {
            awsConf.setProxyHost(proxyHost);
            if(proxyPort >= 0) {
                awsConf.setProxyPort(proxyPort);
            } else if(secureConnections) {
                LOG.warn("Proxy host set without port. Using HTTPS default 443");
                awsConf.setProxyPort(443);
            } else {
                LOG.warn("Proxy host set without port. Using HTTP default 80");
                awsConf.setProxyPort(80);
            }

            endPoint = conf.getTrimmed("fs.s3a.proxy.username");
            String maxThreads = conf.getTrimmed("fs.s3a.proxy.password");
            if(endPoint == null != (maxThreads == null)) {
                coreThreads = "Proxy error: fs.s3a.proxy.username or fs.s3a.proxy.password set without the other.";
                LOG.error(coreThreads);
                throw new IllegalArgumentException(coreThreads);
            }

            awsConf.setProxyUsername(endPoint);
            awsConf.setProxyPassword(maxThreads);
            awsConf.setProxyDomain(conf.getTrimmed("fs.s3a.proxy.domain"));
            awsConf.setProxyWorkstation(conf.getTrimmed("fs.s3a.proxy.workstation"));
            if(LOG.isDebugEnabled()) {
                LOG.debug("Using proxy server {}:{} as user {} with password {} on domain {} as workstation {}", new Object[]{awsConf.getProxyHost(), Integer.valueOf(awsConf.getProxyPort()), String.valueOf(awsConf.getProxyUsername()), awsConf.getProxyPassword(), awsConf.getProxyDomain(), awsConf.getProxyWorkstation()});
            }
        } else if(proxyPort >= 0) {
            endPoint = "Proxy error: fs.s3a.proxy.port set without fs.s3a.proxy.host";
            LOG.error(endPoint);
            throw new IllegalArgumentException(endPoint);
        }

//        this.s3 = new AmazonS3Client(credentials1, awsConf);
//        endPoint = conf.getTrimmed("fs.s3a.endpoint", "");
//        if(!endPoint.isEmpty()) {
//            try {
//                this.s3.setEndpoint(endPoint);
//            } catch (IllegalArgumentException var23) {
//                coreThreads = "Incorrect endpoint: " + var23.getMessage();
//                LOG.error(coreThreads);
//                throw new IllegalArgumentException(coreThreads, var23);
//            }
//        }

        endPoint = conf.get("fs.s3a.endpoint");
        this.s3 = getS3Client(accessKey, secretKey, endPoint);
        this.statistics = getStatistics(getScheme(), this.getClass());
        this.s3.setEndpoint(endPoint);

        this.maxKeys = conf.getInt("fs.s3a.paging.maximum", 5000);
        this.partSize = conf.getLong("fs.s3a.multipart.size", 104857600L);
        this.multiPartThreshold = conf.getInt("fs.s3a.multipart.threshold", 2147483647);
        if(this.partSize < 5242880L) {
            LOG.error("fs.s3a.multipart.size must be at least 5 MB");
            this.partSize = 5242880L;
        }

        if(this.multiPartThreshold < 5242880) {
            LOG.error("fs.s3a.multipart.threshold must be at least 5 MB");
            this.multiPartThreshold = 5242880;
        }

        int maxThreads1 = conf.getInt("fs.s3a.threads.max", 256);
        int coreThreads1 = conf.getInt("fs.s3a.threads.core", 15);
        if(maxThreads1 == 0) {
            maxThreads1 = Runtime.getRuntime().availableProcessors() * 8;
        }

        if(coreThreads1 == 0) {
            coreThreads1 = Runtime.getRuntime().availableProcessors() * 8;
        }

        long keepAliveTime = conf.getLong("fs.s3a.threads.keepalivetime", 60L);
        LinkedBlockingQueue workQueue = new LinkedBlockingQueue(maxThreads1 * conf.getInt("fs.s3a.max.total.tasks", 1000));
        this.threadPoolExecutor = new ThreadPoolExecutor(coreThreads1, maxThreads1, keepAliveTime, TimeUnit.SECONDS, workQueue, newDaemonThreadFactory("s3a-transfer-shared-"));
        this.threadPoolExecutor.allowCoreThreadTimeOut(true);
        TransferManagerConfiguration transferConfiguration = new TransferManagerConfiguration();
        transferConfiguration.setMinimumUploadPartSize(this.partSize);
        transferConfiguration.setMultipartUploadThreshold(this.multiPartThreshold);
        this.transfers = new TransferManager(this.s3, this.threadPoolExecutor);
        this.transfers.setConfiguration(transferConfiguration);
        String cannedACLName = conf.get("fs.s3a.acl.default", "");
        if(!cannedACLName.isEmpty()) {
            this.cannedACL = CannedAccessControlList.valueOf(cannedACLName);
        } else {
            this.cannedACL = null;
        }

//        if(!this.s3.doesBucketExist(this.bucket)) {
//            throw new IOException("Bucket " + this.bucket + " does not exist");
//        } else {
            boolean purgeExistingMultipart = conf.getBoolean("fs.s3a.multipart.purge", false);
            long purgeExistingMultipartAge = conf.getLong("fs.s3a.multipart.purge.age", 14400L);
            if(purgeExistingMultipart) {
                Date purgeBefore = new Date((new Date()).getTime() - purgeExistingMultipartAge * 1000L);
                this.transfers.abortMultipartUploads(this.bucket, purgeBefore);
            }

            this.serverSideEncryptionAlgorithm = conf.get("fs.s3a.server-side-encryption-algorithm");
            this.setConf(conf);
//        }
    }

    public String getScheme() {
        return "s3a";
    }

    public URI getUri() {
        return this.uri;
    }

    @VisibleForTesting
    AmazonS3Client getAmazonS3Client() {
        return this.s3;
    }

    public S3CompatibleClientForHadoop() {
    }

    private String pathToKey(Path path) {
        if(!path.isAbsolute()) {
            path = new Path(this.workingDir, path);
        }

        return path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()?"":path.toUri().getPath().substring(1);
    }

    private Path keyToPath(String key) {
        return new Path("/" + key);
    }

    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("Opening \'{}\' for reading.", f);
        }

        S3AFileStatus fileStatus = this.getFileStatus(f);
        if(fileStatus.isDirectory()) {
            throw new FileNotFoundException("Can\'t open " + f + " because it is a directory");
        } else {
            return new FSDataInputStream(new S3AInputStream(this.bucket, this.pathToKey(f), fileStatus.getLen(), this.s3, this.statistics));
        }
    }

    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        String key = this.pathToKey(f);
        if(!overwrite && this.exists(f)) {
            throw new FileAlreadyExistsException(f + " already exists");
        } else {
            return this.getConf().getBoolean("fs.s3a.fast.upload", false)?new FSDataOutputStream(new S3AFastOutputStream(this.s3, this, this.bucket, key, progress, this.statistics, this.cannedACL, this.serverSideEncryptionAlgorithm, this.partSize, (long)this.multiPartThreshold, this.threadPoolExecutor), this.statistics):new FSDataOutputStream(new S3AOutputStream(this.getConf(), this.transfers, this, this.bucket, key, progress, this.cannedACL, this.statistics, this.serverSideEncryptionAlgorithm), (Statistics)null);
        }
    }

    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }

    public boolean rename(Path src, Path dst) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("Rename path {} to {}", src, dst);
        }

        String srcKey = this.pathToKey(src);
        String dstKey = this.pathToKey(dst);
        if(!srcKey.isEmpty() && !dstKey.isEmpty()) {
            S3AFileStatus srcStatus;
            try {
                srcStatus = this.getFileStatus(src);
            } catch (FileNotFoundException var15) {
                LOG.error("rename: src not found {}", src);
                return false;
            }

            if(srcKey.equals(dstKey)) {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("rename: src and dst refer to the same file or directory");
                }

                return srcStatus.isFile();
            } else {
                S3AFileStatus dstStatus = null;

                try {
                    dstStatus = this.getFileStatus(dst);
                    if(srcStatus.isDirectory() && dstStatus.isFile()) {
                        if(LOG.isDebugEnabled()) {
                            LOG.debug("rename: src is a directory and dst is a file");
                        }

                        return false;
                    }

                    if(dstStatus.isDirectory() && !dstStatus.isEmptyDirectory()) {
                        return false;
                    }
                } catch (FileNotFoundException var16) {
                    Path request = dst.getParent();
                    if(!this.pathToKey(request).isEmpty()) {
                        try {
                            S3AFileStatus objects = this.getFileStatus(dst.getParent());
                            if(!objects.isDirectory()) {
                                return false;
                            }
                        } catch (FileNotFoundException var14) {
                            return false;
                        }
                    }
                }

                if(srcStatus.isFile()) {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("rename: renaming file " + src + " to " + dst);
                    }

                    if(dstStatus != null && dstStatus.isDirectory()) {
                        String keysToDelete = dstKey;
                        if(!dstKey.endsWith("/")) {
                            keysToDelete = dstKey + "/";
                        }

                        String request1 = srcKey.substring(this.pathToKey(src.getParent()).length() + 1);
                        keysToDelete = keysToDelete + request1;
                        this.copyFile(srcKey, keysToDelete);
                    } else {
                        this.copyFile(srcKey, dstKey);
                    }

                    this.delete(src, false);
                } else {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("rename: renaming directory " + src + " to " + dst);
                    }

                    if(!dstKey.endsWith("/")) {
                        dstKey = dstKey + "/";
                    }

                    if(!srcKey.endsWith("/")) {
                        srcKey = srcKey + "/";
                    }

                    if(dstKey.startsWith(srcKey)) {
                        if(LOG.isDebugEnabled()) {
                            LOG.debug("cannot rename a directory to a subdirectory of self");
                        }

                        return false;
                    }

                    ArrayList keysToDelete1 = new ArrayList();
                    if(dstStatus != null && dstStatus.isEmptyDirectory()) {
                        keysToDelete1.add(new KeyVersion(dstKey));
                    }

                    ListObjectsRequest request2 = new ListObjectsRequest();
                    request2.setBucketName(this.bucket);
                    request2.setPrefix(srcKey);
                    request2.setMaxKeys(Integer.valueOf(this.maxKeys));
                    ObjectListing objects1 = this.s3.listObjects(request2);
                    this.statistics.incrementReadOps(1);

                    while(true) {
                        Iterator deleteRequest = objects1.getObjectSummaries().iterator();

                        while(deleteRequest.hasNext()) {
                            S3ObjectSummary summary = (S3ObjectSummary)deleteRequest.next();
                            keysToDelete1.add(new KeyVersion(summary.getKey()));
                            String newDstKey = dstKey + summary.getKey().substring(srcKey.length());
                            this.copyFile(summary.getKey(), newDstKey);
                            if(keysToDelete1.size() == 1000) {
                                DeleteObjectsRequest deleteRequest1 = (new DeleteObjectsRequest(this.bucket)).withKeys(keysToDelete1);
                                this.s3.deleteObjects(deleteRequest1);
                                this.statistics.incrementWriteOps(1);
                                keysToDelete1.clear();
                            }
                        }

                        if(!objects1.isTruncated()) {
                            if(keysToDelete1.size() > 0) {
                                DeleteObjectsRequest deleteRequest2 = (new DeleteObjectsRequest(this.bucket)).withKeys(keysToDelete1);
                                this.s3.deleteObjects(deleteRequest2);
                                this.statistics.incrementWriteOps(1);
                            }
                            break;
                        }

                        objects1 = this.s3.listNextBatchOfObjects(objects1);
                        this.statistics.incrementReadOps(1);
                    }
                }

                if(src.getParent() != dst.getParent()) {
                    this.deleteUnnecessaryFakeDirectories(dst.getParent());
                    this.createFakeDirectoryIfNecessary(src.getParent());
                }

                return true;
            }
        } else {
            if(LOG.isDebugEnabled()) {
                LOG.debug("rename: src or dst are empty");
            }

            return false;
        }
    }

    public boolean delete(Path f, boolean recursive) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("Delete path " + f + " - recursive " + recursive);
        }

        S3AFileStatus status;
        try {
            status = this.getFileStatus(f);
        } catch (FileNotFoundException var11) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Couldn\'t delete " + f + " - does not exist");
            }

            return false;
        }

        String key = this.pathToKey(f);
        if(status.isDirectory()) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("delete: Path is a directory");
            }

            if(!recursive && !status.isEmptyDirectory()) {
                throw new IOException("Path is a folder: " + f + " and it is not an empty directory");
            }

            if(!key.endsWith("/")) {
                key = key + "/";
            }

            if(key.equals("/")) {
                LOG.info("s3a cannot delete the root directory");
                return false;
            }

            if(status.isEmptyDirectory()) {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Deleting fake empty directory");
                }

                this.s3.deleteObject(this.bucket, key);
                this.statistics.incrementWriteOps(1);
            } else {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Getting objects for directory prefix " + key + " to delete");
                }

                ListObjectsRequest request = new ListObjectsRequest();
                request.setBucketName(this.bucket);
                request.setPrefix(key);
                request.setMaxKeys(Integer.valueOf(this.maxKeys));
                ArrayList keys = new ArrayList();
                ObjectListing objects = this.s3.listObjects(request);
                this.statistics.incrementReadOps(1);

                while(true) {
                    Iterator deleteRequest = objects.getObjectSummaries().iterator();

                    while(deleteRequest.hasNext()) {
                        S3ObjectSummary summary = (S3ObjectSummary)deleteRequest.next();
                        keys.add(new KeyVersion(summary.getKey()));
                        if(LOG.isDebugEnabled()) {
                            LOG.debug("Got object to delete " + summary.getKey());
                        }

                        if(keys.size() == 1000) {
                            DeleteObjectsRequest deleteRequest1 = (new DeleteObjectsRequest(this.bucket)).withKeys(keys);
                            this.s3.deleteObjects(deleteRequest1);
                            this.statistics.incrementWriteOps(1);
                            keys.clear();
                        }
                    }

                    if(!objects.isTruncated()) {
                        if(!keys.isEmpty()) {
                            DeleteObjectsRequest deleteRequest2 = (new DeleteObjectsRequest(this.bucket)).withKeys(keys);
                            this.s3.deleteObjects(deleteRequest2);
                            this.statistics.incrementWriteOps(1);
                        }
                        break;
                    }

                    objects = this.s3.listNextBatchOfObjects(objects);
                    this.statistics.incrementReadOps(1);
                }
            }
        } else {
            if(LOG.isDebugEnabled()) {
                LOG.debug("delete: Path is a file");
            }

            this.s3.deleteObject(this.bucket, key);
            this.statistics.incrementWriteOps(1);
        }

        this.createFakeDirectoryIfNecessary(f.getParent());
        return true;
    }

    private void createFakeDirectoryIfNecessary(Path f) throws IOException {
        String key = this.pathToKey(f);
        if(!key.isEmpty() && !this.exists(f)) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Creating new fake directory at " + f);
            }

            this.createFakeDirectory(this.bucket, key);
        }

    }

    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        String key = this.pathToKey(f);
        if(LOG.isDebugEnabled()) {
            LOG.debug("List status for path: " + f);
        }

        ArrayList result = new ArrayList();
        S3AFileStatus fileStatus = this.getFileStatus(f);
        if(fileStatus.isDirectory()) {
            if(!key.isEmpty()) {
                key = key + "/";
            }

            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(this.bucket);
            request.setPrefix(key);
            request.setDelimiter("/");
            request.setMaxKeys(Integer.valueOf(this.maxKeys));
            if(LOG.isDebugEnabled()) {
                LOG.debug("listStatus: doing listObjects for directory " + key);
            }

            ObjectListing objects = this.s3.listObjects(request);
            this.statistics.incrementReadOps(1);

            while(true) {
                Iterator var7 = objects.getObjectSummaries().iterator();

                while(true) {
                    Path keyPath;
                    while(var7.hasNext()) {
                        S3ObjectSummary prefix = (S3ObjectSummary)var7.next();
                        keyPath = this.keyToPath(prefix.getKey()).makeQualified(this.uri, this.workingDir);
                        if(!keyPath.equals(f) && !prefix.getKey().endsWith("_$folder$")) {
                            if(this.objectRepresentsDirectory(prefix.getKey(), prefix.getSize())) {
                                result.add(new S3AFileStatus(true, true, keyPath));
                                if(LOG.isDebugEnabled()) {
                                    LOG.debug("Adding: fd: " + keyPath);
                                }
                            } else {
                                result.add(new S3AFileStatus(prefix.getSize(), dateToLong(prefix.getLastModified()), keyPath, this.getDefaultBlockSize(f.makeQualified(this.uri, this.workingDir))));
                                if(LOG.isDebugEnabled()) {
                                    LOG.debug("Adding: fi: " + keyPath);
                                }
                            }
                        } else if(LOG.isDebugEnabled()) {
                            LOG.debug("Ignoring: " + keyPath);
                        }
                    }

                    var7 = objects.getCommonPrefixes().iterator();

                    while(var7.hasNext()) {
                        String prefix1 = (String)var7.next();
                        keyPath = this.keyToPath(prefix1).makeQualified(this.uri, this.workingDir);
                        if(!keyPath.equals(f)) {
                            result.add(new S3AFileStatus(true, false, keyPath));
                            if(LOG.isDebugEnabled()) {
                                LOG.debug("Adding: rd: " + keyPath);
                            }
                        }
                    }

                    if(!objects.isTruncated()) {
                        return (FileStatus[])result.toArray(new FileStatus[result.size()]);
                    }

                    if(LOG.isDebugEnabled()) {
                        LOG.debug("listStatus: list truncated - getting next batch");
                    }

                    objects = this.s3.listNextBatchOfObjects(objects);
                    this.statistics.incrementReadOps(1);
                    break;
                }
            }
        } else {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Adding: rd (not a dir): " + f);
            }

            result.add(fileStatus);
            return (FileStatus[])result.toArray(new FileStatus[result.size()]);
        }
    }

    public void setWorkingDirectory(Path new_dir) {
        this.workingDir = new_dir;
    }

    public Path getWorkingDirectory() {
        return this.workingDir;
    }

    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("Making directory: " + f);
        }

        try {
            S3AFileStatus e = this.getFileStatus(f);
            if(e.isDirectory()) {
                return true;
            } else {
                throw new FileAlreadyExistsException("Path is a file: " + f);
            }
        } catch (FileNotFoundException var7) {
            Path fPart = f;

            do {
                try {
                    S3AFileStatus key = this.getFileStatus(fPart);
                    if(key.isFile()) {
                        throw new FileAlreadyExistsException(String.format("Can\'t make directory for path \'%s\' since it is a file.", new Object[]{fPart}));
                    }
                } catch (FileNotFoundException var6) {
                    ;
                }

                fPart = fPart.getParent();
            } while(fPart != null);

            String key1 = this.pathToKey(f);
            this.createFakeDirectory(this.bucket, key1);
            return true;
        }
    }

    public S3AFileStatus getFileStatus(Path f) throws IOException {
        String key = this.pathToKey(f);
        if(LOG.isDebugEnabled()) {
            LOG.debug("Getting path status for " + f + " (" + key + ")");
        }

        if(!key.isEmpty()) {
            try {
                ObjectMetadata e2 = this.s3.getObjectMetadata(this.bucket, key);
                this.statistics.incrementReadOps(1);
                if(this.objectRepresentsDirectory(key, e2.getContentLength())) {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Found exact file: fake directory");
                    }

                    return new S3AFileStatus(true, true, f.makeQualified(this.uri, this.workingDir));
                }

                if(LOG.isDebugEnabled()) {
                    LOG.debug("Found exact file: normal file");
                }

                return new S3AFileStatus(e2.getContentLength(), dateToLong(e2.getLastModified()), f.makeQualified(this.uri, this.workingDir), this.getDefaultBlockSize(f.makeQualified(this.uri, this.workingDir)));
            } catch (AmazonServiceException var11) {
                if(var11.getStatusCode() != 404) {
                    this.printAmazonServiceException(var11);
                    throw var11;
                }

                if(!key.endsWith("/")) {
                    try {
                        String e1 = key + "/";
                        ObjectMetadata objects1 = this.s3.getObjectMetadata(this.bucket, e1);
                        this.statistics.incrementReadOps(1);
                        if(this.objectRepresentsDirectory(e1, objects1.getContentLength())) {
                            if(LOG.isDebugEnabled()) {
                                LOG.debug("Found file (with /): fake directory");
                            }

                            return new S3AFileStatus(true, true, f.makeQualified(this.uri, this.workingDir));
                        }

                        LOG.warn("Found file (with /): real file? should not happen: {}", key);
                        return new S3AFileStatus(objects1.getContentLength(), dateToLong(objects1.getLastModified()), f.makeQualified(this.uri, this.workingDir), this.getDefaultBlockSize(f.makeQualified(this.uri, this.workingDir)));
                    } catch (AmazonServiceException var9) {
                        if(var9.getStatusCode() != 404) {
                            this.printAmazonServiceException(var9);
                            throw var9;
                        }
                    } catch (AmazonClientException var10) {
                        throw var10;
                    }
                }
            } catch (AmazonClientException var12) {
                throw var12;
            }
        }

        try {
            if(!key.isEmpty() && !key.endsWith("/")) {
                key = key + "/";
            }

            ListObjectsRequest e = new ListObjectsRequest();
            e.setBucketName(this.bucket);
            e.setPrefix(key);
            e.setDelimiter("/");
            e.setMaxKeys(Integer.valueOf(1));
            ObjectListing objects = this.s3.listObjects(e);
            this.statistics.incrementReadOps(1);
            if(!objects.getCommonPrefixes().isEmpty() || objects.getObjectSummaries().size() > 0) {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Found path as directory (with /): " + objects.getCommonPrefixes().size() + "/" + objects.getObjectSummaries().size());
                    Iterator var5 = objects.getObjectSummaries().iterator();

                    while(var5.hasNext()) {
                        S3ObjectSummary prefix = (S3ObjectSummary)var5.next();
                        LOG.debug("Summary: " + prefix.getKey() + " " + prefix.getSize());
                    }

                    var5 = objects.getCommonPrefixes().iterator();

                    while(var5.hasNext()) {
                        String prefix1 = (String)var5.next();
                        LOG.debug("Prefix: " + prefix1);
                    }
                }

                return new S3AFileStatus(true, false, f.makeQualified(this.uri, this.workingDir));
            }
        } catch (AmazonServiceException var7) {
            if(var7.getStatusCode() != 404) {
                this.printAmazonServiceException(var7);
                throw var7;
            }
        } catch (AmazonClientException var8) {
            throw var8;
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("Not Found: " + f);
        }

        throw new FileNotFoundException("No such file or directory: " + f);
    }

    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
        String key = this.pathToKey(dst);
        if(!overwrite && this.exists(dst)) {
            throw new IOException(dst + " already exists");
        } else {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Copying local file from " + src + " to " + dst);
            }

            LocalFileSystem local = getLocal(this.getConf());
            File srcfile = local.pathToFile(src);
            ObjectMetadata om = new ObjectMetadata();
            if(StringUtils.isNotBlank(this.serverSideEncryptionAlgorithm)) {
                om.setServerSideEncryption(this.serverSideEncryptionAlgorithm);
            }

            PutObjectRequest putObjectRequest = new PutObjectRequest(this.bucket, key, srcfile);
            putObjectRequest.setCannedAcl(this.cannedACL);
            putObjectRequest.setMetadata(om);
            ProgressListener progressListener = new ProgressListener() {
                public void progressChanged(ProgressEvent progressEvent) {
                    switch(progressEvent.getEventCode()) {
                        case 2048:
                            S3CompatibleClientForHadoop.this.statistics.incrementWriteOps(1);
                        default:
                    }
                }
            };
            Upload up = this.transfers.upload(putObjectRequest);
            up.addProgressListener(progressListener);

            try {
                up.waitForUploadResult();
                this.statistics.incrementWriteOps(1);
            } catch (InterruptedException var13) {
                throw new IOException("Got interrupted, cancelling");
            }

            this.finishedWrite(key);
            if(delSrc) {
                local.delete(src, false);
            }

        }
    }

    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if(this.transfers != null) {
                this.transfers.shutdownNow(true);
                this.transfers = null;
            }

        }

    }

    public String getCanonicalServiceName() {
        return null;
    }

    private void copyFile(String srcKey, String dstKey) throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("copyFile " + srcKey + " -> " + dstKey);
        }

        ObjectMetadata srcom = this.s3.getObjectMetadata(this.bucket, srcKey);
        ObjectMetadata dstom = srcom.clone();
        if(StringUtils.isNotBlank(this.serverSideEncryptionAlgorithm)) {
            dstom.setServerSideEncryption(this.serverSideEncryptionAlgorithm);
        }

        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(this.bucket, srcKey, this.bucket, dstKey);
        copyObjectRequest.setCannedAccessControlList(this.cannedACL);
        copyObjectRequest.setNewObjectMetadata(dstom);
        ProgressListener progressListener = new ProgressListener() {
            public void progressChanged(ProgressEvent progressEvent) {
                switch(progressEvent.getEventCode()) {
                    case 2048:
                        S3CompatibleClientForHadoop.this.statistics.incrementWriteOps(1);
                    default:
                }
            }
        };
        Copy copy = this.transfers.copy(copyObjectRequest);
        copy.addProgressListener(progressListener);

        try {
            copy.waitForCopyResult();
            this.statistics.incrementWriteOps(1);
        } catch (InterruptedException var9) {
            throw new IOException("Got interrupted, cancelling");
        }
    }

    private boolean objectRepresentsDirectory(String name, long size) {
        return !name.isEmpty() && name.charAt(name.length() - 1) == 47 && size == 0L;
    }

    private static long dateToLong(Date date) {
        return date == null?0L:date.getTime();
    }

    public void finishedWrite(String key) throws IOException {
        this.deleteUnnecessaryFakeDirectories(this.keyToPath(key).getParent());
    }

    private void deleteUnnecessaryFakeDirectories(Path f) throws IOException {
        while(true) {
            try {
                String key = this.pathToKey(f);
                if(key.isEmpty()) {
                    break;
                }

                S3AFileStatus status = this.getFileStatus(f);
                if(status.isDirectory() && status.isEmptyDirectory()) {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Deleting fake directory " + key + "/");
                    }

                    this.s3.deleteObject(this.bucket, key + "/");
                    this.statistics.incrementWriteOps(1);
                }
            } catch (AmazonServiceException | FileNotFoundException var4) {
                ;
            }

            if(f.isRoot()) {
                break;
            }

            f = f.getParent();
        }

    }

    private void createFakeDirectory(String bucketName, String objectName) throws AmazonClientException, AmazonServiceException {
        if(!objectName.endsWith("/")) {
            this.createEmptyObject(bucketName, objectName + "/");
        } else {
            this.createEmptyObject(bucketName, objectName);
        }

    }

    private void createEmptyObject(String bucketName, String objectName) throws AmazonClientException, AmazonServiceException {
        InputStream im = new InputStream() {
            public int read() throws IOException {
                return -1;
            }
        };
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(0L);
        if(StringUtils.isNotBlank(this.serverSideEncryptionAlgorithm)) {
            om.setServerSideEncryption(this.serverSideEncryptionAlgorithm);
        }

        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName, im, om);
        putObjectRequest.setCannedAcl(this.cannedACL);
        this.s3.putObject(putObjectRequest);
        this.statistics.incrementWriteOps(1);
    }

    /** @deprecated */
    @Deprecated
    public long getDefaultBlockSize() {
        return this.getConf().getLong("fs.s3a.block.size", 33554432L);
    }

    private void printAmazonServiceException(AmazonServiceException ase) {
        LOG.info("Caught an AmazonServiceException {}", ase.toString());
        LOG.info("Error Message: {}", ase.getMessage());
        LOG.info("HTTP Status Code: {}", Integer.valueOf(ase.getStatusCode()));
        LOG.info("AWS Error Code: {}", ase.getErrorCode());
        LOG.info("Error Type: {}", ase.getErrorType());
        LOG.info("Request ID: {}", ase.getRequestId());
        LOG.info("Stack", ase);
    }
}
