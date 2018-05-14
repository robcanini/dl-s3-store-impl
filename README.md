# dl-s3-store-impl
Make Liferay 6.2 CE GA6 S3 document library migration works

Simply package project with Maven:

```
$ mvn clean package -U
```

Then copy the artifact with dependencies into the

```
/webapps/ROOT/WEB-INF/lib/
```
directory inside tomcat installation.

Then edit your portal-ext.proprties file and add following lines with your custom S3 properties:

```
dl.store.s3.access.key=YOUR_ACCESS_KEY
dl.store.s3.secret.key=YOUR_SECRET_KEY
dl.store.s3.bucket.name=YOUR_BUCKET_NAME
dl.store.impl=it.miriade.liferay.documentlibrary.store.S3V4Store
```

If you previously uploaded some files into document_library, then you must copy all document_library content of your filesystem into root s3 bucket folder.

Reboot the portal.

Congratulations! Your Liferay 6.2 will now use S3 as document library store system.
