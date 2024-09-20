package org.pentaho.di.sdk.samples.jobentries.demo;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entries.sftp.JobEntrySFTP;
import org.pentaho.di.job.entries.sftp.SFTPClient;
import org.pentaho.di.job.entry.JobEntryInterface;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@JobEntry(
        id = "DemoJobEntry",
        name = "DemoJobEntry.Name",
        description = "DemoJobEntry.TooltipDesc",
        image = "org/pentaho/di/sdk/samples/jobentries/demo/resources/demo.svg",
        categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.Conditions",
        i18nPackageName = "org.pentaho.di.sdk.samples.jobentries.demo",
        documentationUrl = "DemoJobEntry.DocumentationURL",
        casesUrl = "DemoJobEntry.CasesURL",
        forumUrl = "DemoJobEntry.ForumURL"
)
public class JobEntrySFTPList extends JobEntrySFTP implements Cloneable, JobEntryInterface {

    private static final Class<?> PKG = JobEntrySFTPList.class;

    public Result execute(Result previousResult, int nr) {
        long filesRetrieved = 0L;
        if (this.parentJobMeta.getNamedClusterEmbedManager() != null) {
            this.parentJobMeta.getNamedClusterEmbedManager().passEmbeddedMetastoreKey(this, this.parentJobMeta.getEmbeddedMetastoreProviderKey());
        }

        if (this.log.isDetailed()) {
            this.logDetailed(BaseMessages.getString(PKG, "JobSFTP.Log.StartJobEntry"));
        }

        String realServerName;

        SFTPClient sftpclient = null;
        realServerName = this.environmentSubstitute(getServerName());
        String realServerPort = this.environmentSubstitute(getServerPort());
        String realUsername = this.environmentSubstitute(getUserName());
        String realPassword = Encr.decryptPasswordOptionallyEncrypted(this.environmentSubstitute(getPassword()));
        String realSftpDirString = this.environmentSubstitute(getScpDirectory());
        String realWildcard = this.environmentSubstitute(getWildcard());
        String realKeyFilename = null;
        String realPassPhrase = null;

        try {
            try {
                if (this.isUseKeyFile()) {
                    realKeyFilename = this.environmentSubstitute(this.getKeyFilename());
                    Result var56;
                    if (Utils.isEmpty(realKeyFilename)) {
                        this.logError(BaseMessages.getString(PKG, "JobSFTP.Error.KeyFileMissing"));
                        previousResult.setNrErrors(1L);
                        var56 = previousResult;
                        return var56;
                    }

                    if (!KettleVFS.fileExists(realKeyFilename)) {
                        this.logError(BaseMessages.getString(PKG, "JobSFTP.Error.KeyFileNotFound", realKeyFilename));
                        previousResult.setNrErrors(1L);
                        var56 = previousResult;
                        return var56;
                    }

                    realPassPhrase = this.environmentSubstitute(this.getKeyPassPhrase());
                }

                sftpclient = new SFTPClient(InetAddress.getByName(realServerName), Const.toInt(realServerPort, 22), realUsername, realKeyFilename, realPassPhrase);
                if (this.log.isDetailed()) {
                    this.logDetailed(BaseMessages.getString(PKG, "JobSFTP.Log.OpenedConnection", realServerName, realServerPort, realUsername));
                }

                sftpclient.setCompression(this.getCompression());
                String realProxyHost = this.environmentSubstitute(this.getProxyHost());
                if (!Utils.isEmpty(realProxyHost)) {
                    String password = this.getRealPassword(this.getProxyPassword());
                    sftpclient.setProxy(realProxyHost, this.environmentSubstitute(this.getProxyPort()), this.environmentSubstitute(this.getProxyUsername()), password, this.getProxyType());
                }

                sftpclient.login(realPassword);
                if (!Utils.isEmpty(realSftpDirString)) {
                    try {
                        sftpclient.chdir(realSftpDirString);
                    } catch (Exception var49) {
                        this.logError(BaseMessages.getString(PKG, "JobSFTP.Error.CanNotFindRemoteFolder", realSftpDirString));
                        throw new Exception(var49);
                    }

                    if (this.log.isDetailed()) {
                        this.logDetailed(BaseMessages.getString(PKG, "JobSFTP.Log.ChangedDirectory", realSftpDirString));
                    }
                }

                String[] fileList = sftpclient.dir();
                if (fileList == null) {
                    previousResult.setResult(true);
                    if (this.log.isDetailed()) {
                        this.logDetailed(BaseMessages.getString(PKG, "JobSFTP.Log.Found", "0"));
                    }

                    return previousResult;
                }

                List<String> resultList;
                if (Utils.isEmpty(realWildcard)) {
                    resultList = Arrays.asList(fileList);
                } else {
                    Pattern pattern = Pattern.compile(realWildcard);
                    resultList = Arrays.stream(fileList)
                            .filter(name -> pattern.matcher(name).matches())
                            .collect(Collectors.toList());
                }

                previousResult.setResult(true);
                previousResult.setNrFilesRetrieved(filesRetrieved);
            } catch (Exception e) {
                previousResult.setNrErrors(1L);
                this.logError(BaseMessages.getString(PKG, "JobSFTP.Error.GettingFiles", e.getMessage()));
                this.logError(Const.getStackTracker(e));
            }

            return previousResult;
        } finally {
            try {
                if (sftpclient != null) {
                    sftpclient.disconnect();
                }
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public String getDialogClassName() {
        return JobEntrySFTPListDialog.class.getName();
    }

    @Override
    public JobEntrySFTPList clone() {
        return (JobEntrySFTPList) super.clone();
    }
}
