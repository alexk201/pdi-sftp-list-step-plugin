package org.pentaho.di.sdk.samples.jobentries.demo;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.job.entries.sftp.JobEntrySFTPDialog;

public class JobEntrySFTPListDialog extends JobEntrySFTPDialog implements JobEntryDialogInterface {

    private static final Class<?> PKG = JobEntrySFTPList.class; // for i18n purposes

    public JobEntrySFTPListDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
    }
}
