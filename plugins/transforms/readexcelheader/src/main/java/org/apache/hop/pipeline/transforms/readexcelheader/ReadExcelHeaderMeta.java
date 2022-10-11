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

import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.eclipse.swt.widgets.Shell;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/** Meta data for the sample transform. */
@Transform(
    id = "ReadExcelHeader",
    name = "i18n::ReadExcelHeader.Step.Name",
    description = "i18n::ReadExcelHeader.Step.Description",
    image = "REH.svg",
    categoryDescription = "ReadExcelHeader.Category",
    documentationUrl = "" /*url to your documentation */)

@InjectionSupported(localizationPrefix = "ReadExcelHeaderMeta.Injection.")
public class ReadExcelHeaderMeta extends BaseTransformMeta<ReadExcelHeader, ReadExcelHeaderData> {

  private static final Class<?> PKG = ReadExcelHeaderMeta.class; // Needed by Translator
  //public static final String SAMPLE_TEXT_FIELD_NAME = "Value";

  /*@HopMetadataProperty(
      key = "readexcelheader_text",
      injectionKeyDescription = "ReadExcelHeaderTransform.Injection.SampleText.Description")*/

  /** Field to read from */
  //@Injection(name = "FIELD_OF_FILENAMES")
  @HopMetadataProperty(
          key = "FIELD_OF_FILENAMES",
          injectionKeyDescription = "ReadExcelHeaderMeta.Injection.FILE_FIELDNAME")
  private String filenameField;
  //@Injection(name = "ROW_TO_START_ON")
  @HopMetadataProperty(
          key = "ROW_TO_START_ON",
          injectionKeyDescription = "ReadExcelHeaderMeta.Injection.ROW_TO_START_ON")
  private String startRow;
  //@Injection(name = "NUMBER_OF_ROWS_TO_SAMPLE")

  @HopMetadataProperty(
          key = "NUMBER_OF_ROWS_TO_SAMPLE",
          injectionKeyDescription = "ReadExcelHeaderMeta.Injection.NUMBER_OF_ROWS_TO_SAMPLE")
  private String sampleRows;

  private String startRowFieldName;

  private boolean filefield;
  private boolean startrowfield;

  private String dynamicWildcardField;
  private String dynamicExcludeWildcardField;
  private boolean dynamicIncludeSubFolders;
  /** Flag : do not fail if no file */
  private boolean doNotFailIfNoFile;

  public static final String[] RequiredFilesDesc = new String[] { Messages.getString("System.Combo.No"),
          Messages.getString("System.Combo.Yes") };

  public static final String[] RequiredFilesCode = new String[] { "N", "Y" };
  private static final String NO = "N";
  private static final String YES = "Y";

  /** Array of filenames */
  private String[] fileName;

  /** Wildcard or filemask (regular expression) */
  private String[] fileMask;

  /** Wildcard or filemask to exclude (regular expression) */
  private String[] excludeFileMask;

  /** Flag indicating that a row number field should be included in the output */
  private boolean includeFilesCount;

  /** Array of boolean values as string, indicating if a file is required. */
  private String[] fileRequired;

  /**
   * Array of boolean values as string, indicating if we need to fetch sub
   * folders.
   */
  private String[] includeSubFolders;

  public ReadExcelHeaderMeta() {
    super();
  }

  /**
   * Called by Spoon to get a new instance of the SWT dialog for the step. A
   * standard implementation passing the arguments to the constructor of the step
   * dialog is recommended.
   *
   * @param shell     an SWT Shell
   * @param meta      description of the step
   * @param transMeta description of the the transformation
   * @param name      the name of the step
   * @return new instance of a dialog for this step
   */
  public ITransformDialog getDialog(Shell shell, IVariables variables, ITransformMeta meta, PipelineMeta transMeta, String name) {
    return new ReadExcelHeaderDialog(shell, variables, meta, transMeta, name);
  }

  /**
   * Called by PDI to get a new instance of the step implementation. A standard
   * implementation passing the arguments to the constructor of the step class is
   * recommended.
   *
   * @param stepMeta          description of the step
   * @param stepDataInterface instance of a step data class
   * @param cnr               copy number
   * @param transMeta         description of the transformation
   * @param disp              runtime implementation of the transformation
   * @return the new instance of a step implementation
   */
  public ITransform getTransform(TransformMeta stepMeta, ReadExcelHeaderMeta meta, ReadExcelHeaderData stepDataInterface, int cnr, PipelineMeta transMeta,
                            Pipeline disp) {
    return new ReadExcelHeader(stepMeta, meta, stepDataInterface, cnr, transMeta, disp);
  }

  /**
   * Called by PDI to get a new instance of the step data class.
   */
  public ITransformData getTransformData() {
    return new ReadExcelHeaderData();
  }

  /**
   * This method is called every time a new step is created and should
   * allocate/set the step configuration to sensible defaults. The values set here
   * will be used by Spoon when a new step is created.
   */
  @Override
  public void setDefault() {
    filenameField = "";
    startRowFieldName = "";
    startRow = "0";
    sampleRows = "10";
    filefield = false;
    startrowfield = false;
    dynamicWildcardField = "";
    dynamicIncludeSubFolders = false;
    dynamicExcludeWildcardField = "";
    doNotFailIfNoFile = false;

    int nrFiles = 0;

    allocate(nrFiles);

    /*for (int i = 0; i < nrFiles; i++) {
      fileName[i] = "filename" + (i + 1);
      fileMask[i] = "";
      excludeFileMask[i] = "";
      fileRequired[i] = RequiredFilesCode[0];
      includeSubFolders[i] = RequiredFilesCode[0];
    }*/
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
          throws HopXmlException {
    readData(transformNode);
  }

  /**
   * This method is used when a step is duplicated in Spoon. It needs to return a
   * deep copy of this step meta object. Be sure to create proper deep copies if
   * the step configuration is stored in modifiable objects.
   *
   * See org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta.clone() for an
   * example on creating a deep copy.
   *
   * @return a deep copy of this
   */
  @Override
  public Object clone() {
    ReadExcelHeaderMeta retval = (ReadExcelHeaderMeta) super.clone();
    normilizeAllocation();

    retval.filenameField = filenameField;
    retval.startRowFieldName = startRowFieldName;
    retval.startRow = startRow;
    retval.sampleRows = sampleRows;

    int nrFiles = fileName.length;

    retval.allocate(nrFiles);

    System.arraycopy(fileName, 0, retval.fileName, 0, nrFiles);
    System.arraycopy(fileMask, 0, retval.fileMask, 0, nrFiles);
    System.arraycopy(excludeFileMask, 0, retval.excludeFileMask, 0, nrFiles);
    System.arraycopy(fileRequired, 0, retval.fileRequired, 0, nrFiles);
    System.arraycopy(includeSubFolders, 0, retval.includeSubFolders, 0, nrFiles);

    return retval;
  }

  /**
   * This method is called to determine the changes the step is making to the
   * row-stream. To that end a RowMetaInterface object is passed in, containing
   * the row-stream structure as it is when entering the step. This method must
   * apply any changes the step makes to the row stream. Usually a step adds
   * fields to the row-stream.
   *
   * @param inputRowMeta the row structure coming in to the step
   * @param name         the name of the step making the changes
   * @param info         row structures of any info steps coming in
   * @param nextStep     the description of a step this step is passing rows to
   * @param space        the variable space for resolving variables
   * @param metaStore    the metaStore to optionally read from
   */
  public void getFields(IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextStep,
                        IVariables space, IHopMetadataProvider metaStore) throws HopTransformException {
    /*
     * This implementation appends the outputField to the row-stream
     */
    // a value meta object contains the meta data for a field
    IValueMeta vWorkbook = new ValueMetaString("workbookName", 500, -1);

    // the name of the step that adds this field
    vWorkbook.setOrigin(name);

    vWorkbook.setTrimType(IValueMeta.TRIM_TYPE_NONE);

    // modify the row structure and add the field this step generates
    inputRowMeta.addValueMeta(vWorkbook);

    // a value meta object contains the meta data for a field
    IValueMeta vSheet = new ValueMetaString("sheetName", 500, -1);

    // the name of the step that adds this field
    vSheet.setOrigin(name);

    vSheet.setTrimType(IValueMeta.TRIM_TYPE_NONE);

    // modify the row structure and add the field this step generates
    inputRowMeta.addValueMeta(vSheet);

    // a value meta object contains the meta data for a field
    IValueMeta vColumnName = new ValueMetaString("columnName", 500, -1);

    // the name of the step that adds this field
    vColumnName.setOrigin(name);

    vColumnName.setTrimType(IValueMeta.TRIM_TYPE_NONE);

    // modify the row structure and add the field this step generates
    inputRowMeta.addValueMeta(vColumnName);

    // a value meta object contains the meta data for a field
    IValueMeta vColumnType = new ValueMetaString("columnType", 500, -1);

    // the name of the step that adds this field
    vColumnType.setOrigin(name);

    vColumnType.setTrimType(IValueMeta.TRIM_TYPE_NONE);

    // modify the row structure and add the field this step generates
    inputRowMeta.addValueMeta(vColumnType);

    // a value meta object contains the meta data for a field
    IValueMeta vColumnDataFormat = new ValueMetaString("columnDataFormat", 500, -1);

    // the name of the step that adds this field
    vColumnDataFormat.setOrigin(name);

    vColumnDataFormat.setTrimType(IValueMeta.TRIM_TYPE_NONE);

    // modify the row structure and add the field this step generates
    inputRowMeta.addValueMeta(vColumnDataFormat);
  }

  public FileInputList getFiles(IVariables space) {
    normilizeAllocation();
    return FileInputList.createFileList(space, fileName, fileMask, excludeFileMask, fileRequired,
            includeSubFolderBoolean());
  }

  public FileInputList getDynamicFileList(IVariables space, String[] filename, String[] filemask,
                                          String[] excludefilemask, String[] filerequired, boolean[] includesubfolders) {
    normilizeAllocation();
    return FileInputList.createFileList(space, filename, filemask, excludefilemask, filerequired,
            includesubfolders);
  }

  private boolean[] includeSubFolderBoolean() {
    normilizeAllocation();
    int len = fileName.length;
    boolean[] includeSubFolderBoolean = new boolean[len];
    for (int i = 0; i < len; i++) {
      includeSubFolderBoolean[i] = YES.equalsIgnoreCase(includeSubFolders[i]);
    }
    return includeSubFolderBoolean;
  }

  /**
   * This method is called when the user selects the "Verify Transformation"
   * option in Spoon. A list of remarks is passed in that this method should add
   * to. Each remark is a comment, warning, error, or ok. The method should
   * perform as many checks as necessary to catch design-time errors.
   *
   * Typical checks include: - verify that all mandatory configuration is given -
   * verify that the step receives any input, unless it's a row generating step -
   * verify that the step does not receive any input if it does not take them into
   * account - verify that the step finds fields it relies on in the row-stream
   *
   * @param remarks   the list of remarks to append to
   * @param pipelineMeta the description of the transformation
   * @param transformMeta  the description of the step
   * @param prev      the structure of the incoming row-stream
   * @param input     names of steps sending input to the step
   * @param output    names of steps this step is sending output to
   * @param info      fields coming in from info steps
   */
  @Override
  public void check(List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
                    String[] input, String[] output, IRowMeta info, IVariables space, IHopMetadataProvider metadataProvider) {

    CheckResult cr;

    if (prev == null || prev.size() == 0) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!",
              transformMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK,
              "Step is connected to previous one, receiving " + prev.size() + " fields", transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this step!
    if (input.length > 0) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", transformMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", transformMeta);
      remarks.add(cr);
    }

    if (!filenameField.isEmpty()) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step received a filename.", transformMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No filename received!", transformMeta);
      remarks.add(cr);
    }

    if (!startRow.isEmpty() && !startrowfield) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step received a row to start on.", transformMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No start row received!", transformMeta);
      remarks.add(cr);
    }

    if (!startRowFieldName.isEmpty() && startrowfield) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step received a field with a row to start on.", transformMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No start row field received!", transformMeta);
      remarks.add(cr);
    }

    if (!sampleRows.isEmpty()) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step received number of rows to sample on.", transformMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No number of sample rows received!", transformMeta);
      remarks.add(cr);
    }
  }

  public String getFilenameField() {
    return filenameField;
  }

  public void setFilenameField(final String filenameField) {
    this.filenameField = filenameField;
  }

  public String getStartRowFieldName() {
    return startRowFieldName;
  }

  public void setStartRowFieldName(final String startRowFieldName) {
    this.startRowFieldName = startRowFieldName;
  }

  public String getStartRow() {
    return startRow;
  }

  public void setStartRow(final String StartRow) {
    this.startRow = StartRow;
  }

  public String getSampleRows() {
    return sampleRows;
  }

  public void setSampleRows(String sampleRows) {
    this.sampleRows = sampleRows;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(500);
    normilizeAllocation();

    retval.append("   " + XmlHandler.addTagValue("filenamefield", filenameField));
    retval.append("   " + XmlHandler.addTagValue("startrow", startRow));
    retval.append("   " + XmlHandler.addTagValue("sampleRows", sampleRows));
    retval.append("    ").append(XmlHandler.addTagValue("filefield", filefield));
    retval.append("    ").append(XmlHandler.addTagValue("startrowfield", startrowfield));
    retval.append("    ").append(XmlHandler.addTagValue("startrowfieldname", startRowFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("wildcard_Field", dynamicWildcardField));
    retval.append("    ").append(XmlHandler.addTagValue("exclude_wildcard_Field", dynamicExcludeWildcardField));
    retval.append("    ").append(XmlHandler.addTagValue("dynamic_include_subfolders", dynamicIncludeSubFolders));
    retval.append("    ").append(XmlHandler.addTagValue("doNotFailIfNoFile", doNotFailIfNoFile));

    retval.append("    <file>").append(Const.CR);
    for (int i = 0; i < fileName.length; i++) {
      retval.append("      ").append(XmlHandler.addTagValue("file_name", fileName[i]));
      retval.append("      ").append(XmlHandler.addTagValue("file_mask", fileMask[i]));
      retval.append("      ").append(XmlHandler.addTagValue("exclude_file_mask", excludeFileMask[i]));
      retval.append("      ").append(XmlHandler.addTagValue("file_required", fileRequired[i]));
      retval.append("      ").append(XmlHandler.addTagValue("include_subfolders", includeSubFolders[i]));
      //parentTransformMeta.getParentPipelineMeta().getNamedClusterEmbedManager().registerUrl(fileName[i]);
    }
    retval.append("    </file>").append(Const.CR);

    return retval.toString();
  }

  /**
   * @return Returns the excludeFileMask.
   */
  public String[] getExcludeFileMask() {
    return excludeFileMask;
  }

  /**
   * @param excludeFileMask The excludeFileMask to set.
   */
  public void setExcludeFileMask(String[] excludeFileMask) {
    this.excludeFileMask = excludeFileMask;
  }

  /**
   * @return Returns the output filename_Field.
   */
  public String getFileNameField() {
    return filenameField;
  }

  /**
   * @param filenameField The output filename_field to set.
   */
  public void setFileNameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /**
   * @return Returns the fileMask.
   */
  public String[] getFileMask() {
    return fileMask;
  }

  public void setFileRequired(String[] fileRequiredin) {
    this.fileRequired = new String[fileRequiredin.length];
    for (int i = 0; i < fileRequiredin.length; i++) {
      this.fileRequired[i] = getRequiredFilesCode(fileRequiredin[i]);
    }
  }

  public String[] getIncludeSubFolders() {
    return includeSubFolders;
  }

  public void setIncludeSubFolders(String[] includeSubFoldersin) {
    this.includeSubFolders = new String[includeSubFoldersin.length];
    for (int i = 0; i < includeSubFoldersin.length; i++) {
      this.includeSubFolders[i] = getRequiredFilesCode(includeSubFoldersin[i]);
    }
  }

  public String getRequiredFilesCode(String tt) {
    if (tt == null) {
      return RequiredFilesCode[0];
    }
    if (tt.equals(RequiredFilesDesc[1])) {
      return RequiredFilesCode[1];
    } else {
      return RequiredFilesCode[0];
    }
  }

  public String getRequiredFilesDesc(String tt) {
    if (tt == null) {
      return RequiredFilesDesc[0];
    }
    if (tt.equals(RequiredFilesCode[1])) {
      return RequiredFilesDesc[1];
    } else {
      return RequiredFilesDesc[0];
    }
  }

  /**
   * @param fileMask The fileMask to set.
   */
  public void setFileMask(String[] fileMask) {
    this.fileMask = fileMask;
  }

  /**
   * @return Returns the fileName.
   */
  public String[] getFileName() {


    for (String s: fileName) {
      //Do your stuff here
      System.out.println("fileName return : " + s);
    }
    return fileName;
  }

  /**
   * @param fileName The fileName to set.
   */
  public void setFileName(String[] fileName) {

    for (String s: fileName) {
      //Do your stuff here
      System.out.println("fileName save : " + s);
    }

    this.fileName = fileName;
  }

  /**
   * @return Returns the includeCountFiles.
   */
  public boolean includeCountFiles() {
    return includeFilesCount;
  }

  /**
   * @param includeFilesCount The "includes files count" flag to set.
   */
  public void setIncludeCountFiles(boolean includeFilesCount) {
    this.includeFilesCount = includeFilesCount;
  }

  public String[] getFileRequired() {
    return this.fileRequired;
  }

  /**
   * @return Returns the File field.
   */
  public boolean isFileField() {
    return filefield;
  }

  /**
   * @param filefield The file field to set.
   */
  public void setFileField(boolean filefield) {
    this.filefield = filefield;
  }

  /**
   * @return Returns the start row field.
   */
  public boolean isStartRowField() {
    return startrowfield;
  }

  /**
   * @param startrowfield The start row field to set.
   */
  public void setStartRowField(boolean startrowfield) {
    this.startrowfield = startrowfield;
  }

  /**
   * @return the doNotFailIfNoFile flag
   */
  public boolean isdoNotFailIfNoFile() {
    return doNotFailIfNoFile;
  }

  /**
   * @param doNotFailIfNoFile the doNotFailIfNoFile to set
   */
  public void setdoNotFailIfNoFile(boolean doNotFailIfNoFile) {
    this.doNotFailIfNoFile = doNotFailIfNoFile;
  }

  /**
   * @param dynamicWildcardField The dynamic wildcard field to set.
   */
  public void setDynamicWildcardField(String dynamicWildcardField) {
    this.dynamicWildcardField = dynamicWildcardField;
  }

  /**
   * @return Returns the dynamic wildcard field (from previous steps)
   */
  public String getDynamicWildcardField() {
    return dynamicWildcardField;
  }

  public String getDynamicExcludeWildcardField() {
    return this.dynamicExcludeWildcardField;
  }

  public void setDynamicExcludeWildcardField(String dynamicExcludeWildcardField) {
    this.dynamicExcludeWildcardField = dynamicExcludeWildcardField;
  }

  public boolean isDynamicIncludeSubFolders() {
    return dynamicIncludeSubFolders;
  }

  public void setDynamicIncludeSubFolders(boolean dynamicIncludeSubFolders) {
    this.dynamicIncludeSubFolders = dynamicIncludeSubFolders;
  }
  // For compatibility with 7.x
  @Override
  public String getDialogClassName() {
    return ReadExcelHeaderDialog.class.getName();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {

      filenameField = XmlHandler.getTagValue(transformNode, "filenamefield");
      startRow = XmlHandler.getTagValue(transformNode, "startrow");
      sampleRows = XmlHandler.getTagValue(transformNode, "sampleRows");

      filefield = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "filefield"));
      startrowfield = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "startrowfield"));
      startRowFieldName = XmlHandler.getTagValue(transformNode, "startrowfieldname");
      doNotFailIfNoFile = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "doNotFailIfNoFile"));
      dynamicWildcardField = XmlHandler.getTagValue(transformNode, "wildcard_Field");
      dynamicExcludeWildcardField = XmlHandler.getTagValue(transformNode, "exclude_wildcard_Field");
      dynamicIncludeSubFolders = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "dynamic_include_subfolders"));

      Node filenode = XmlHandler.getSubNode(transformNode, "file");

      int nrfiles = XmlHandler.countNodes(filenode, "file_name");

      allocate(nrfiles);

      for (int i = 0; i < nrfiles; i++) {
        Node filenamenode = XmlHandler.getSubNodeByNr(filenode, "file_name", i);
        Node filemasknode = XmlHandler.getSubNodeByNr(filenode, "file_mask", i);
        Node excludefilemasknode = XmlHandler.getSubNodeByNr(filenode, "exclude_file_mask", i);
        Node fileRequirednode = XmlHandler.getSubNodeByNr(filenode, "file_required", i);
        if (!YES.equalsIgnoreCase(fileRequired[i])) {
          fileRequired[i] = NO;
        }
        Node includeSubFoldersnode = XmlHandler.getSubNodeByNr(filenode, "include_subfolders", i);
        if (!YES.equalsIgnoreCase(includeSubFolders[i])) {
          includeSubFolders[i] = NO;
        }
        fileName[i] = XmlHandler.getNodeValue(filenamenode);
        fileMask[i] = XmlHandler.getNodeValue(filemasknode);
        excludeFileMask[i] = XmlHandler.getNodeValue(excludefilemasknode);
        fileRequired[i] = XmlHandler.getNodeValue(fileRequirednode);
        includeSubFolders[i] = XmlHandler.getNodeValue(includeSubFoldersnode);
      }

    } catch (Exception e) {
      throw new HopXmlException("Unable to read transform information from XML", e);
    }
  }

  public String[] normilizeArray(String[] array, int length) {
    String[] newArray = new String[length];
    if (array != null) {
      if (array.length <= length) {
        System.arraycopy(array, 0, newArray, 0, array.length);
      } else {
        System.arraycopy(array, 0, newArray, 0, length);
      }
    }
    return newArray;
  }

  public void normilizeAllocation() {
    int nrfiles = 0;

    if (fileName != null) {
      nrfiles = fileName.length;
    } else {
      fileName = new String[0];
    }

    fileMask = normilizeArray(fileMask, nrfiles);
    excludeFileMask = normilizeArray(excludeFileMask, nrfiles);
    fileRequired = normilizeArray(fileRequired, nrfiles);
    includeSubFolders = normilizeArray(includeSubFolders, nrfiles);
  }
  public void allocate(int nrfiles) {

    fileName = new String[nrfiles];
    fileMask = new String[nrfiles];
    excludeFileMask = new String[nrfiles];
    fileRequired = new String[nrfiles];
    includeSubFolders = new String[nrfiles];
  }

  @AfterInjection
  public void afterInjectionSynchronization() {
    this.normilizeAllocation();
  }

}
