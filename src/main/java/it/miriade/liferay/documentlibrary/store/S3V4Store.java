package it.miriade.liferay.documentlibrary.store;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.liferay.portal.kernel.exception.PortalException;
import com.liferay.portal.kernel.exception.SystemException;
import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.util.CharPool;
import com.liferay.portal.kernel.util.DateUtil;
import com.liferay.portal.kernel.util.FileUtil;
import com.liferay.portal.kernel.util.LocaleUtil;
import com.liferay.portal.kernel.util.PropsUtil;
import com.liferay.portal.kernel.util.StreamUtil;
import com.liferay.portal.kernel.util.StringBundler;
import com.liferay.portal.kernel.util.StringPool;
import com.liferay.portal.kernel.util.SystemProperties;
import com.liferay.portal.kernel.util.Time;
import com.liferay.portal.kernel.util.Validator;
import com.liferay.portal.kernel.uuid.PortalUUIDUtil;
import com.liferay.portal.util.PropsValues;
import com.liferay.portlet.documentlibrary.NoSuchFileException;
import com.liferay.portlet.documentlibrary.store.BaseStore;

/**
 *
 * @author Miriade Srl
 * @see http://www.miriade.it
 *
 */
public class S3V4Store extends BaseStore {

	private AmazonS3 S3;
	private static final String bucketName = PropsUtil.get("dl.store.s3.bucket.name");
	private Log logger = LogFactoryUtil.getLog(getClass().getSimpleName());

	private static final String _TEMP_DIR_NAME = "/liferay/s3";
	private static final String _TEMP_DIR_PATTERN = "/yyyy/MM/dd/HH/";
	private int _calledGetFileCount;

	protected String getKey(long companyId, long repositoryId) {
		StringBundler sb = new StringBundler(4);

		sb.append(companyId);
		sb.append(StringPool.SLASH);
		sb.append(repositoryId);

		return sb.toString();
	}

	protected String getKey(long companyId, long repositoryId, String fileName) {

		StringBundler sb = new StringBundler(4);

		sb.append(companyId);
		sb.append(StringPool.SLASH);
		sb.append(repositoryId);
		sb.append(getNormalizedFileName(fileName));

		return sb.toString();
	}

	protected String getKey(long companyId, long repositoryId, String fileName, String versionLabel) {

		StringBundler sb = new StringBundler(6);

		sb.append(companyId);
		sb.append(StringPool.SLASH);
		sb.append(repositoryId);
		sb.append(getNormalizedFileName(fileName));
		sb.append(StringPool.SLASH);
		sb.append(versionLabel);

		return sb.toString();
	}

	protected String getNormalizedFileName(String fileName) {
		String normalizedFileName = fileName;

		if (!fileName.startsWith(StringPool.SLASH)) {
			normalizedFileName = StringPool.SLASH + normalizedFileName;
		}

		if (fileName.endsWith(StringPool.SLASH)) {
			normalizedFileName = normalizedFileName.substring(0, normalizedFileName.length() - 1);
		}

		return normalizedFileName;
	}

	protected String getHeadVersionLabel(long companyId, long repositoryId, String fileName)
			throws NoSuchFileException {

		ListObjectsRequest listRequest = new ListObjectsRequest();
		listRequest.setBucketName(bucketName);
		listRequest.setPrefix(getKey(companyId, repositoryId, fileName));
		listRequest.setDelimiter(null);
		ObjectListing listing = S3.listObjects(listRequest);
		List<S3ObjectSummary> s3Objects = listing.getObjectSummaries();

		String[] keys = new String[s3Objects.size()];

		for (int i = 0; i < s3Objects.size(); i++) {
			S3ObjectSummary s3Object = s3Objects.get(i);

			keys[i] = s3Object.getKey();
		}

		if (keys.length > 0) {
			Arrays.sort(keys);

			String headKey = keys[keys.length - 1];

			int x = headKey.lastIndexOf(CharPool.SLASH);

			return headKey.substring(x + 1);
		} else {
			throw new NoSuchFileException(fileName);
		}
	}

	protected File getTempFile(S3Object s3Object, String fileName) throws IOException {

		StringBundler sb = new StringBundler(5);

		sb.append(SystemProperties.get(SystemProperties.TMP_DIR));
		sb.append(_TEMP_DIR_NAME);
		sb.append(DateUtil.getCurrentDate(_TEMP_DIR_PATTERN, LocaleUtil.getDefault()));
		sb.append(getNormalizedFileName(fileName));

		Date lastModifiedDate = s3Object.getObjectMetadata().getLastModified();

		sb.append(lastModifiedDate.getTime());

		String tempFileName = sb.toString();

		File tempFile = new File(tempFileName);

		if (tempFile.exists() && (tempFile.lastModified() >= lastModifiedDate.getTime())) {

			return tempFile;
		}

		InputStream inputStream = s3Object.getObjectContent();

		if (inputStream == null) {
			throw new IOException("S3 object input stream is null");
		}

		OutputStream outputStream = null;

		try {
			File parentFile = tempFile.getParentFile();

			FileUtil.mkdirs(parentFile);

			outputStream = new FileOutputStream(tempFile);

			StreamUtil.transfer(inputStream, outputStream);
		} finally {
			StreamUtil.cleanUp(inputStream, outputStream);
		}

		return tempFile;
	}

	protected void cleanUpTempFiles() {
		_calledGetFileCount++;

		if (_calledGetFileCount < PropsValues.DL_STORE_S3_TEMP_DIR_CLEAN_UP_FREQUENCY) {

			return;
		}

		synchronized (this) {
			if (_calledGetFileCount == 0) {
				return;
			}

			_calledGetFileCount = 0;

			String tempDirName = SystemProperties.get(SystemProperties.TMP_DIR) + _TEMP_DIR_NAME;

			File tempDir = new File(tempDirName);

			long lastModified = System.currentTimeMillis();

			lastModified -= (PropsValues.DL_STORE_S3_TEMP_DIR_CLEAN_UP_EXPUNGE * Time.DAY);

			cleanUpTempFiles(tempDir, lastModified);
		}
	}

	protected void cleanUpTempFiles(File file, long lastModified) {
		if (!file.isDirectory()) {
			return;
		}

		String[] fileNames = FileUtil.listDirs(file);

		if (fileNames.length == 0) {
			if (file.lastModified() < lastModified) {
				FileUtil.deltree(file);

				return;
			}
		} else {
			for (String fileName : fileNames) {
				cleanUpTempFiles(new File(file, fileName), lastModified);
			}

			String[] subfileNames = file.list();

			if (subfileNames.length == 0) {
				FileUtil.deltree(file);

				return;
			}
		}
	}

	public S3V4Store() {
		BasicAWSCredentials credentials = new BasicAWSCredentials(PropsUtil.get("dl.store.s3.access.key"),
				PropsUtil.get("dl.store.s3.secret.key"));
		S3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion(Regions.EU_CENTRAL_1).build();

		if (logger.isDebugEnabled()) {
			logger.debug("S3V4Store activated");
		}
	}

	@Override
	public void addDirectory(long arg0, long arg1, String arg2) throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("addDirectory");
		}
	}

	@Override
	public void addFile(long companyId, long repositoryId, String fileName, InputStream is)
			throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("addFile");
		}
		try {
			PutObjectRequest putRequest = new PutObjectRequest(bucketName,
					getKey(companyId, repositoryId, fileName, VERSION_DEFAULT), is, null);
			S3.putObject(putRequest);
		} finally {
			StreamUtil.cleanUp(is);
		}
	}

	@Override
	public void checkRoot(long arg0) throws SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("checkRoot");
		}
	}

	@Override
	public void deleteDirectory(long companyId, long repositoryId, String dirName)
			throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("deleteDirectory");
		}
		ListObjectsRequest listRequest = new ListObjectsRequest();
		listRequest.setBucketName(bucketName);
		listRequest.setPrefix(getKey(companyId, repositoryId, dirName));
		listRequest.setDelimiter(null);
		ObjectListing listing = S3.listObjects(listRequest);
		List<S3ObjectSummary> summaries = listing.getObjectSummaries();
		summaries.forEach(summary -> {
			S3.deleteObject(bucketName, summary.getKey());
		});
	}

	@Override
	public void deleteFile(long companyId, long repositoryId, String fileName) throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("deleteFile");
		}
		ListObjectsRequest listRequest = new ListObjectsRequest();
		listRequest.setBucketName(bucketName);
		listRequest.setPrefix(getKey(companyId, repositoryId, fileName));
		listRequest.setDelimiter(null);
		ObjectListing listing = S3.listObjects(listRequest);
		List<S3ObjectSummary> summaries = listing.getObjectSummaries();
		summaries.forEach(summary -> {
			S3.deleteObject(bucketName, summary.getKey());
		});
	}

	@Override
	public void deleteFile(long companyId, long repositoryId, String fileName, String versionLabel)
			throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("deleteFile");
		}
		DeleteObjectRequest deleteRequest = new DeleteObjectRequest(bucketName,
				getKey(companyId, repositoryId, fileName, versionLabel));
		S3.deleteObject(deleteRequest);
	}

	@Override
	public File getFile(long companyId, long repositoryId, String fileName, String versionLabel)
			throws PortalException, SystemException {

		try {
			if (Validator.isNull(versionLabel)) {
				versionLabel = getHeadVersionLabel(companyId, repositoryId, fileName);
			}

			S3Object s3Object = S3.getObject(bucketName, getKey(companyId, repositoryId, fileName, versionLabel));

			File tempFile = getTempFile(s3Object, fileName);

			cleanUpTempFiles();

			return tempFile;
		} catch (IOException ioe) {
			throw new SystemException(ioe);
		}
	}

	@Override
	public InputStream getFileAsStream(long companyId, long repositoryId, String fileName, String versionLabel)
			throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("getFileAsStream");
		}
		if (Validator.isNull(versionLabel)) {
			versionLabel = getHeadVersionLabel(companyId, repositoryId, fileName);
		}

		S3Object s3Object = S3.getObject(bucketName, getKey(companyId, repositoryId, fileName, versionLabel));

		return s3Object.getObjectContent();
	}

	public String[] getFileNames(long companyId, long repositoryId) throws SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("getFileNames");
		}
		ListObjectsRequest listRequest = new ListObjectsRequest();
		listRequest.setBucketName(bucketName);
		listRequest.setPrefix(getKey(companyId, repositoryId));
		listRequest.setDelimiter(null);

		ObjectListing listing = S3.listObjects(listRequest);

		return getFileNames(listing.getObjectSummaries());
	}

	protected String[] getFileNames(List<S3ObjectSummary> summaries) {
		List<String> fileNames = new ArrayList<String>();

		for (S3ObjectSummary s3Object : summaries) {
			String fileName = getFileName(s3Object.getKey());

			fileNames.add(fileName);
		}

		return fileNames.toArray(new String[fileNames.size()]);
	}

	protected String getFileName(String key) {

		// Convert /${companyId}/${repositoryId}/${dirName}/${fileName}
		// /${versionLabel} to /${dirName}/${fileName}

		int x = key.indexOf(CharPool.SLASH);

		x = key.indexOf(CharPool.SLASH, x + 1);

		int y = key.lastIndexOf(CharPool.SLASH);

		return key.substring(x, y);
	}

	@Override
	public long getFileSize(long companyId, long repositoryId, String fileName)
			throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("getFileSize");
		}
		String versionLabel = getHeadVersionLabel(companyId, repositoryId, fileName);

		ObjectMetadata objectDetails = S3.getObjectMetadata(bucketName,
				getKey(companyId, repositoryId, fileName, versionLabel));

		return objectDetails.getContentLength();
	}

	@Override
	public boolean hasDirectory(long companyId, long repositoryId, String dirName)
			throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("hasDirectory");
		}
		return true;
	}

	@Override
	public boolean hasFile(long companyId, long repositoryId, String fileName, String versionLabel)
			throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("hasFile");
		}
		ListObjectsRequest listRequest = new ListObjectsRequest();
		listRequest.setBucketName(bucketName);
		listRequest.setPrefix(getKey(companyId, repositoryId, fileName, versionLabel));
		listRequest.setDelimiter(null);

		ObjectListing listing = S3.listObjects(listRequest);

		return listing.getObjectSummaries().size() > 0;
	}

	@Override
	public void move(String arg0, String arg1) throws SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("move");
		}
	}

	@Override
	public void updateFile(long companyId, long repositoryId, long newRepositoryId, String fileName)
			throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("updateFile");
		}
		ListObjectsRequest listRequest = new ListObjectsRequest();
		listRequest.setBucketName(bucketName);
		listRequest.setPrefix(getKey(companyId, repositoryId, fileName));
		listRequest.setDelimiter(null);
		ObjectListing listing = S3.listObjects(listRequest);

		File tempFile = null;

		for (S3ObjectSummary oldS3ObjectSummary : listing.getObjectSummaries()) {
			String oldKey = oldS3ObjectSummary.getKey();

			S3Object oldS3Object = S3.getObject(bucketName, oldKey);

			tempFile = new File(
					SystemProperties.get(SystemProperties.TMP_DIR) + File.separator + PortalUUIDUtil.generate());

			try {
				FileUtil.write(tempFile, oldS3Object.getObjectContent());
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}

			String newPrefix = getKey(companyId, newRepositoryId);

			int x = oldKey.indexOf(CharPool.SLASH);

			x = oldKey.indexOf(CharPool.SLASH, x + 1);

			String newKey = newPrefix + oldKey.substring(x);

			PutObjectRequest putRequest = new PutObjectRequest(bucketName, newKey, tempFile);

			S3.putObject(putRequest);

			S3.deleteObject(bucketName, oldKey);
		}
	}

	public void updateFile(long companyId, long repositoryId, String fileName, String newFileName)
			throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("updateFile");
		}

		File tempFile = null;

		ListObjectsRequest listRequest = new ListObjectsRequest();
		listRequest.setBucketName(bucketName);
		listRequest.setPrefix(getKey(companyId, repositoryId, fileName));
		listRequest.setDelimiter(null);
		ObjectListing listing = S3.listObjects(listRequest);

		for (S3ObjectSummary oldS3ObjectSummary : listing.getObjectSummaries()) {
			String oldKey = oldS3ObjectSummary.getKey();

			S3Object oldS3Object = S3.getObject(bucketName, oldKey);

			tempFile = new File(
					SystemProperties.get(SystemProperties.TMP_DIR) + File.separator + PortalUUIDUtil.generate());

			try {
				FileUtil.write(tempFile, oldS3Object.getObjectContent());
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}

			String newPrefix = getKey(companyId, repositoryId, newFileName);

			int x = oldKey.indexOf(StringPool.SLASH);

			x = oldKey.indexOf(CharPool.SLASH, x + 1);
			x = oldKey.indexOf(CharPool.SLASH, x + 1);

			String newKey = newPrefix + oldKey.substring(x);

			PutObjectRequest putRequest = new PutObjectRequest(bucketName, newKey, tempFile);

			S3.putObject(putRequest);

			S3.deleteObject(bucketName, oldKey);
		}
	}

	@Override
	public void updateFile(long companyId, long repositoryId, String fileName, String versionLabel, InputStream is)
			throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("updateFile");
		}
		try {
			PutObjectRequest putRequest = new PutObjectRequest(bucketName,
					getKey(companyId, repositoryId, fileName, versionLabel), is, null);
			S3.putObject(putRequest);
		} finally {
			StreamUtil.cleanUp(is);
		}
	}

	@Override
	public String[] getFileNames(long companyId, long repositoryId, String dirName) throws PortalException, SystemException {
		if (logger.isDebugEnabled()) {
			logger.debug("getFileNames");
		}
		ListObjectsRequest listRequest = new ListObjectsRequest();
		listRequest.setBucketName(bucketName);
		listRequest.setPrefix(getKey(companyId, repositoryId, dirName));
		listRequest.setDelimiter(null);

		ObjectListing listing = S3.listObjects(listRequest);

		return getFileNames(listing.getObjectSummaries());
	}

}
