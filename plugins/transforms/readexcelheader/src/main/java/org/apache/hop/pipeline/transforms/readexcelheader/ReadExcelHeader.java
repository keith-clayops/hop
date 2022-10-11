/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.readexcelheader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.poi.xssf.usermodel.*;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Transform That contains the basic skeleton needed to create your own plugin */
public class ReadExcelHeader extends BaseTransform<ReadExcelHeaderMeta, ReadExcelHeaderData> {

  private static final Class<?> PKG = ReadExcelHeader.class; // Needed by Translator

  private int startRow;
  private int sampleRows;
  //private ReadExcelHeaderMeta meta;
  //private ReadExcelHeaderData data;

  InputStream file1InputStream = null;
  XSSFWorkbook workbook1 = null;

  private String filePath;

  public ReadExcelHeader(TransformMeta stepMeta, ReadExcelHeaderMeta meta, ReadExcelHeaderData stepDataInterface, int copyNr, PipelineMeta transMeta,
                         Pipeline trans) {
    super(stepMeta, meta, stepDataInterface, copyNr, transMeta, trans);

    //meta = (ReadExcelHeaderMeta) meta2;
    //data = (ReadExcelHeaderData) stepDataInterface;

  }

  public boolean init() {

    //meta = (ReadExcelHeaderMeta) smi;
    //data = (ReadExcelHeaderData) sdi;

    if (super.init()) {
      first = true;

      if (!meta.isFileField()) {
        data.files = meta.getFiles(this);

        if (data.files == null || data.files.nrOfFiles() == 0) {
          logError(Messages.getString("ReadExcelHeaderDialog.Log.NoFiles"));
          return false;
        }
        data.filessize = data.files.nrOfFiles();
        try {
          handleMissingFiles();
          // Create the output row meta-data
          data.outputRowMeta = new RowMeta();
          meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider); // get
          // the
          // metadata
          // populated

        } catch (Exception e) {
          logError("Error initializing step: " + e.toString());
          logError(Const.getStackTracker(e));
          return false;
        }
      }
      data.rownr = 0;
      data.filenr = 0;
      data.totalpreviousfields = 0;

      try {
        sampleRows = Integer.parseInt(resolve(meta.getSampleRows()));
        logDebug("Received StartRow: " + startRow + " SampleRows: " + sampleRows);
      } catch (NumberFormatException nfe) {
        logError("SampleRows couldn't be parsed");
        logDebug("SampleRow: " + meta.getSampleRows());
        return false;
      }

      return true;
    }
    return false;
  }

  public boolean processRow() throws HopException {

    // safely cast the step settings (meta) and runtime info (data) to specific
    // implementations
    //meta = (ReadExcelHeaderMeta) smi;
    //data = (ReadExcelHeaderData) sdi;

    // get incoming row, getRow() potentially blocks waiting for more rows, returns
    // null if no more rows expected
    Object[] r;// = getRow();

    try {
      Object[] outputRowData = getOneRow();
      if (outputRowData == null) {
        setOutputDone(); // signal end to receiver(s)
        return false; // end of data or error.
      }
      r = outputRowData;

      logDebug("Is file field used? " + (meta.isFileField() ? "Yes" : "No"));
      filePath = data.file.toString();

      getHeader(r);
      // indicate that processRow() should be called again
      return true;

    } catch (HopException e) {

      logError(Messages.getString("ReadExcelHeaderDialog.ErrorInStepRunning", e.getMessage()));
      setErrors(1);
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }
    // return true;

  }

  private Object[] getOneRow() throws HopException {
    if (!openNextFile()) {
      return null;
    }

    // Build an empty row based on the meta-data
    Object[] r;
    try {
      // Create new row or clone
      if (meta.isFileField()) {
        r = data.readrow.clone();
        r = RowDataUtil.resizeArray(r, data.outputRowMeta.size());
      } else {
        if (meta.isStartRowField()) {
          r = data.readrow.clone();
          r = RowDataUtil.resizeArray(r, data.outputRowMeta.size());
        } else {
          r = RowDataUtil.allocateRowData(data.outputRowMeta.size());
        }
      }

      r[data.totalpreviousfields] = data.rownr;

      incrementLinesInput();

    } catch (Exception e) {
      throw new HopException("Unable to read row from file", e);
    }
    int indexOfStartRowField = -1;
    try {
      if (meta.isStartRowField()) {
        indexOfStartRowField = data.inputRowMeta.indexOfValue(meta.getStartRowFieldName());
        if (indexOfStartRowField < 0) {
          // The field is unreachable !
          logError(Messages.getString("ReadExcelHeaderDialog.Log.ErrorFindingStartRowField",
                  meta.getFileNameField()));
          throw new HopException(Messages.getString(
                  "ReadExcelHeaderDialog.Exception.CouldnotFindStartRowField", meta.getStartRowFieldName()));
        }
        startRow = Integer.parseInt(data.inputRowMeta.getString(r, indexOfStartRowField));
      } else {
        startRow = Integer.parseInt(resolve(meta.getStartRow()));
      }
    } catch (NumberFormatException nfe) {
      logError("StartRow couldn't be parsed");
      logDebug("Startrow: " + meta.getStartRow());
      logDebug("Startrow from field: " + data.inputRowMeta.getString(r, indexOfStartRowField));
      logDebug("Startrow field index: " + indexOfStartRowField);
      throw new HopValueException("StartRow couldn't be parsed.");
    } catch (Exception e) {
      logError("Isstartrowfield: " + meta.isStartRowField());
      logError("startrowfieldname: " + meta.getStartRowFieldName());
      logError("startrow: " + meta.getStartRow());
      logError("data.inputRowMeta null?: " + (data.inputRowMeta == null ? "yes" : "no"));
      throw new HopValueException("Some other error");
    }

    return r;
  }

  private boolean openNextFile() {
    // if ( data.filenr >= data.filessize ) {
    // setOutputDone();
    // return false;
    // }

    try {
      if (!meta.isFileField()) {
        if (meta.isStartRowField()) {
          data.readrow = getRow(); // Get row from input rowset & set row busy!
          if (data.readrow == null) {
            if (log.isDetailed()) {
              logDetailed(Messages.getString("ReadExcelHeaderDialog.Log.FinishedProcessing"));
            }
            return false;
          }

          data.inputRowMeta = getInputRowMeta();
          data.outputRowMeta = data.inputRowMeta.clone();
          meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

          // Get total previous fields
          data.totalpreviousfields = data.inputRowMeta.size();

          // Check is start field is provided
          if (Utils.isEmpty(meta.getStartRowFieldName())) {
            logError(Messages.getString("ReadExcelHeaderDialog.Log.NoStartRowField"));
            throw new HopException(Messages.getString("ReadExcelHeaderDialog.Log.NoStartRowField"));
          }

        }

        if (data.filenr >= data.filessize) {
          // finished processing!

          if (log.isDetailed()) {
            logDetailed(Messages.getString("ReadExcelHeaderDialog.Log.FinishedProcessing"));
          }
          return false;
        }

        data.file = data.files.getFile((int) data.filenr);

      } else {
        if (data.filenr >= data.filessize) {
          data.readrow = getRow(); // Get row from input rowset & set row busy!
        }
        if (data.readrow == null) {
          if (log.isDetailed()) {
            logDetailed(Messages.getString("ReadExcelHeaderDialog.Log.FinishedProcessing"));
          }
          return false;
        }

        if (first) {
          first = false;

          data.inputRowMeta = getInputRowMeta();
          data.outputRowMeta = data.inputRowMeta.clone();
          meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

          // Get total previous fields
          data.totalpreviousfields = data.inputRowMeta.size();

          // Check is filename field is provided
          if (Utils.isEmpty(meta.getFileNameField())) {
            logError(Messages.getString("ReadExcelHeaderDialog.Log.NoField"));
            throw new HopException(Messages.getString("ReadExcelHeaderDialog.Log.NoField"));
          }

          // cache the position of the field
          if (data.indexOfFilenameField < 0) {
            data.indexOfFilenameField = getInputRowMeta().indexOfValue(meta.getFileNameField());
            if (data.indexOfFilenameField < 0) {
              // The field is unreachable !
              logError(Messages.getString("ReadExcelHeaderDialog.Log.ErrorFindingField",
                      meta.getFileNameField()));
              throw new HopException(Messages.getString(
                      "ReadExcelHeaderDialog.Exception.CouldnotFindField", meta.getFileNameField()));
            }
          }
          // If wildcard field is specified, Check if field exists
          if (!Utils.isEmpty(meta.getDynamicWildcardField())) {
            if (data.indexOfWildcardField < 0) {
              data.indexOfWildcardField = data.inputRowMeta.indexOfValue(meta.getDynamicWildcardField());
              if (data.indexOfWildcardField < 0) {
                // The field is unreachable !
                logError(Messages.getString("ReadExcelHeaderDialog.Log.ErrorFindingField") + "["
                        + meta.getDynamicWildcardField() + "]");
                throw new HopException(
                        Messages.getString("ReadExcelHeaderDialog.Exception.CouldnotFindField",
                                meta.getDynamicWildcardField()));
              }
            }
          }
          // If ExcludeWildcard field is specified, Check if field exists
          if (!Utils.isEmpty(meta.getDynamicExcludeWildcardField())) {
            if (data.indexOfExcludeWildcardField < 0) {
              data.indexOfExcludeWildcardField = data.inputRowMeta
                      .indexOfValue(meta.getDynamicExcludeWildcardField());
              if (data.indexOfExcludeWildcardField < 0) {
                // The field is unreachable !
                logError(Messages.getString("ReadExcelHeaderDialog.Log.ErrorFindingField") + "["
                        + meta.getDynamicExcludeWildcardField() + "]");
                throw new HopException(
                        Messages.getString("ReadExcelHeaderDialog.Exception.CouldnotFindField",
                                meta.getDynamicExcludeWildcardField()));
              }
            }
          }

        } // End if first

        if (data.filenr >= data.filessize) {

          String filename = getInputRowMeta().getString(data.readrow, data.indexOfFilenameField);
          String wildcard = "";
          if (data.indexOfWildcardField >= 0) {
            wildcard = getInputRowMeta().getString(data.readrow, data.indexOfWildcardField);
          }
          String excludewildcard = "";
          if (data.indexOfExcludeWildcardField >= 0) {
            excludewildcard = getInputRowMeta().getString(data.readrow, data.indexOfExcludeWildcardField);
          }
          if (log.isDetailed()) {
            logDetailed(Messages.getString("ReadExcelHeaderDialog.Log.FilenameInStream",
                    meta.getFileNameField(), filename));
            logDetailed(Messages.getString("ReadExcelHeaderDialog.Log.FilenameInStream",
                    meta.getDynamicWildcardField(), wildcard));
            logDetailed(Messages.getString("ReadExcelHeaderDialog.Log.FilenameInStream",
                    meta.getDynamicExcludeWildcardField(), excludewildcard));
          }

          String[] filesname = { filename };
          String[] filesmask = { wildcard };
          String[] excludefilesmask = { excludewildcard };
          String[] filesrequired = { "N" };
          boolean[] includesubfolders = { meta.isDynamicIncludeSubFolders() };

          data.files = meta.getDynamicFileList(this, filesname, filesmask, excludefilesmask, filesrequired,
                  includesubfolders);
          data.filessize = data.files.nrOfFiles();
          if (meta.isdoNotFailIfNoFile() && data.files.nrOfFiles() == 0) {
            logBasic(Messages.getString("ReadExcelHeaderDialog.Log.NoFiles"));
            return false;
          }
          data.filenr = 0;
        }

        if (data.filessize > 0) {
          // data.filessize = data.files.nrOfFiles();
          data.file = data.files.getFile(data.filenr);

          String tempFilename = HopVfs.getFilename(data.file);
          data.file = HopVfs.getFileObject(tempFilename);

          // Init Row number
          if (meta.isFileField()) {
            data.rownr = 0;
          }
        }
      }

      // Move file pointer ahead!
      data.filenr++;

      if (log.isDetailed()) {
        logDetailed(Messages.getString("ReadExcelHeaderDialog.Log.OpeningFile", data.file.toString()));
      }

      // if ( log.isDetailed() ) {
      // logDetailed( Messages.getString( "ReadExcelHeaderDialog.Log.FileOpened",
      // data.file.toString() ) );
      // }

    } catch (Exception e) {
      logError(Messages.getString("ReadExcelHeaderDialog.Log.UnableToOpenFile", "" + e.toString()));
      stopAll();
      setErrors(1);
      return false;
    }
    return true;
  }

  private void handleMissingFiles() throws HopException {
    if (!meta.isdoNotFailIfNoFile() && data.files.nrOfFiles() == 0) {
      logBasic(Messages.getString("ReadExcelHeaderDialog.Log.NoFile"));
      return;
    }
    List<FileObject> nonExistantFiles = data.files.getNonExistantFiles();

    if (nonExistantFiles.size() != 0) {
      String message = FileInputList.getRequiredFilesDescription(nonExistantFiles);
      logBasic("ERROR: Missing " + message);
      throw new HopException("Following required files are missing: " + message);
    }

    List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
    if (nonAccessibleFiles.size() != 0) {
      String message = FileInputList.getRequiredFilesDescription(nonAccessibleFiles);
      logBasic("WARNING: Not accessible " + message);
      throw new HopException("Following required files are not accessible: " + message);
    }
  }

  private void getHeader(Object[] r) throws HopTransformException {
    filePath = (filePath.contains("file://") ? filePath.substring(7) : filePath);
    logDebug("cleansed filepath is: " + filePath);

    try {
      // String tempfilename = KettleVFS.getFilename(filePath);
      FileObject fileObject = HopVfs.getFileObject(HopVfs.getFilename(data.file));
      if (fileObject instanceof LocalFile) {
        //This might reduce memory usage
        logDebug("Local file");
        String localFilename = HopVfs.getFilename(fileObject);
        File excelFile = new File(localFilename);
        file1InputStream = new FileInputStream(excelFile);
        workbook1 = new XSSFWorkbook(file1InputStream);
      } else {
        logDebug("VFS file");
        file1InputStream = HopVfs.getInputStream(HopVfs.getFilename(data.file));
        workbook1 = new XSSFWorkbook(file1InputStream);
      }
      logDebug("successfully read file");

    } catch (Exception e) {
      logDebug("Couldn't get file");
      logDebug(e.getMessage());
      throw new HopTransformException("Couldn't read file provided");
    }

    if (workbook1 == null) {
      log.logDebug("Supplied file: " + filePath);
      throw new HopTransformException("Could not read the file provided.");
    }

    for (int i = 0; i < workbook1.getNumberOfSheets(); i++) {
      XSSFSheet sheet;
      XSSFRow row;

      try {
        sheet = workbook1.getSheetAt(i);
      } catch (Exception e) {
        log.logError("Unable to read sheet\n" + e.getMessage());
        throw new HopTransformException("Could not read sheet with number: " + i);
      }
      try {
        row = sheet.getRow(startRow);
        log.logDebug("Found a sheet with the corresponding header row (from/to): " + row.getFirstCellNum() + "/"
                + row.getLastCellNum());
      } catch (Exception e) {
        log.logDebug("Unable to read row.\nMaybe the row given is empty.\n Providing empty row.");
        Object[] outputRow = RowDataUtil.createResizedCopy(r, data.outputRowMeta.size());
        int lastMeta = data.outputRowMeta.size();
        outputRow[lastMeta - 5] = (new File(filePath)).getName();
        log.logRowlevel("Got workbook name: " + outputRow[lastMeta - 5] + " setting in "
                + String.valueOf(lastMeta - 5));
        outputRow[lastMeta - 4] = workbook1.getSheetName(i);
        log.logRowlevel(
                "Got sheet name: " + outputRow[lastMeta - 4] + " setting in " + String.valueOf(lastMeta - 4));
        outputRow[lastMeta - 3] = "NO DATA";
        outputRow[lastMeta - 2] = "NO DATA";
        outputRow[lastMeta - 1] = "NO DATA";

        try {
          workbook1.close();
          // file1InputStream.close();
        } catch (Exception wce) {
          new HopException("Could not dispose workbook.\n" + wce.getMessage());
        }
        try {
          file1InputStream.close();
        } catch (IOException fce) {
          new HopException("Could not dispose FileInputStream.\n" + fce.getMessage());
        }
        continue;
        // throw new KettleStepException("Could not read row with startrow: " +
        // startRow);
      }
      for (short j = row.getFirstCellNum(); j < row.getLastCellNum(); j++) {
        // generate output row, make it correct size
        Object[] outputRow = RowDataUtil.createResizedCopy(r, data.outputRowMeta.size());

        int lastMeta = data.outputRowMeta.size();
        try {
          log.logRowlevel("Processing the next cell with number: " + j);
          outputRow[lastMeta - 5] = data.file;
          log.logRowlevel("Got workbook name: " + outputRow[lastMeta - 5] + " setting in "
                  + String.valueOf(lastMeta - 5));
        } catch (Exception e) {
          log.logError("Some error while getting the file string" + data.file);

          log.logError(e.getMessage());
          throw new HopTransformException(e.getMessage());
        }
        try {
          outputRow[lastMeta - 4] = workbook1.getSheetName(i);
          log.logRowlevel("Got sheet name: " + outputRow[lastMeta - 4] + " setting in "
                  + String.valueOf(lastMeta - 4));
        } catch (Exception e) {
          log.logError("Some error while getting the values. With Sheetnumber:" + String.valueOf(i));

          log.logError(e.getMessage());
          throw new HopTransformException(e.getMessage());
        }
        try{
          outputRow[lastMeta - 3] = row.getCell(j).toString();
          log.logRowlevel("Got cell header: " + outputRow[lastMeta - 3] + " setting in "
                  + String.valueOf(lastMeta - 3));
        } catch (Exception e) {
          log.logDebug("Some error while getting the values. With Sheetnumber:" + String.valueOf(i)
                  + " and Column number:" + String.valueOf(j));
          outputRow[lastMeta - 3] = "NO DATA";
          // log.logError(e.getMessage());
          // throw new KettleStepException(e.getMessage());
        }


        Map<String, String[]> cellInfo = new HashMap<>();
        log.logRowlevel("Startrow is: " + startRow);
        log.logRowlevel("Samplerows is: " + sampleRows);
        for (int k = startRow + 1; k <= startRow + sampleRows; k++) {
          log.logRowlevel("Going into loop for getting cell info with k= " + k);
          try {
            XSSFCell cell = sheet.getRow(k).getCell(row.getCell(j).getColumnIndex());
            log.logRowlevel("Adding type and style to list: '" + cell.getCellTypeEnum().toString() + "/"
                    + cell.getCellStyle().getDataFormatString() + "'");
            cellInfo.put(cell.getCellTypeEnum().toString() + cell.getCellStyle().getDataFormatString(),
                    new String[] { cell.getCellTypeEnum().toString(),
                            cell.getCellStyle().getDataFormatString() });
          } catch (Exception e) {
            log.logRowlevel("Couldn't get Field info in row " + String.valueOf(k));
          }
        }
        if (cellInfo.size() == 0) {
          outputRow[lastMeta - 2] = "NO DATA";
          outputRow[lastMeta - 1] = "NO DATA";
        } else if (cellInfo.size() > 1) {
          outputRow[lastMeta - 2] = "STRING";
          outputRow[lastMeta - 1] = "Mixed";
        } else {
          String[] info = (String[]) cellInfo.values().toArray()[0];
          outputRow[lastMeta - 2] = info[0];
          outputRow[lastMeta - 1] = info[1];
        }

        // put the row to the output row stream
        log.logRowlevel(
                "Created the following row: " + Arrays.toString(outputRow) + " ;map size=" + cellInfo.size());
        putRow(data.outputRowMeta, outputRow);
      }
      // log progress if it is time to to so
      if (checkFeedback(getLinesRead())) {
        logBasic("Processed Rows: " + getLinesRead()); // Some basic logging
      }
    }
    try {
      workbook1.close();
    } catch (Exception e) {
      new HopException("Could not dispose workbook.\n" + e.getMessage());
    }

    try {
      file1InputStream.close();
    } catch (IOException e) {
      new HopException("Could not dispose FileInputStream.\n" + e.getMessage());
    }
  }

  public void dispose() {

    // Casting to step-specific implementation classes is safe
    //meta = (ReadExcelHeaderMeta) smi;
    //data = (ReadExcelHeaderData) sdi;

    // // Add any step-specific initialization that may be needed here
    // try {
    // file1InputStream.close();
    // } catch (IOException e) {
    // log.logError("Could not dispose FileInputStream.\n" + e.getMessage());
    // }

    // Call superclass dispose()
    //super.dispose(smi, sdi);
    super.dispose();
  }


}
