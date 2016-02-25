/*
 * Copyright (c) 2012, The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.broadinstitute.sting.queue

import function.QFunction
import java.io.File
import org.broadinstitute.sting.commandline._
import org.broadinstitute.sting.queue.util._
import org.broadinstitute.sting.queue.engine.{QGraphSettings, QGraph}
import collection.JavaConversions._
import org.broadinstitute.sting.utils.classloader.PluginManager
import org.broadinstitute.sting.utils.exceptions.UserException
import org.broadinstitute.sting.utils.io.IOUtils
import org.broadinstitute.sting.utils.help.ApplicationDetails
import java.util.{ResourceBundle, Arrays}
import org.broadinstitute.sting.utils.text.TextFormattingUtils
import org.apache.commons.io.FilenameUtils

/**
 * Entry point of Queue.  Compiles and runs QScripts passed in to the command line.
 */
object QCommandLine extends Logging {
  /**
   * Main.
   * @param argv Arguments.
   */
  def main(argv: Array[String]) {
    val qCommandLine = new QCommandLine

    val shutdownHook = new Thread {
      override def run() {
        logger.info("Shutting down jobs. Please wait...")
        qCommandLine.shutdown()
      }
    }

    Runtime.getRuntime.addShutdownHook(shutdownHook)

    try {
      CommandLineProgram.start(qCommandLine, argv)
      try {
        Runtime.getRuntime.removeShutdownHook(shutdownHook)
        qCommandLine.shutdown()
      } catch {
        case _ => /* ignore, example 'java.lang.IllegalStateException: Shutdown in progress' */
      }
      if (CommandLineProgram.result != 0)
        System.exit(CommandLineProgram.result);
    } catch {
      case e: Exception => CommandLineProgram.exitSystemWithError(e)
    }
  }
}

/**
 * Entry point of Queue.  Compiles and runs QScripts passed in to the command line.
 */
class QCommandLine extends CommandLineProgram with Logging {
  @Input(fullName="script", shortName="S", doc="QScript scala file", required=true)
  @ClassType(classOf[File])
  var scripts = Seq.empty[File]

  @ArgumentCollection
  val settings = new QGraphSettings

  private val qScriptManager = new QScriptManager
  private val qGraph = new QGraph
  private var qScriptClasses: File = _
  private var shuttingDown = false

  private lazy val pluginManager = {
    qScriptClasses = IOUtils.tempDir("Q-Classes-", "", settings.qSettings.tempDirectory)
    qScriptManager.loadScripts(scripts, qScriptClasses)
    new PluginManager[QScript](classOf[QScript], Seq(qScriptClasses.toURI.toURL))
  }

  QFunction.parsingEngine = new ParsingEngine(this)

  /**
   * Takes the QScripts passed in, runs their script() methods, retrieves their generated
   * functions, and then builds and runs a QGraph based on the dependencies.
   */
  def execute = {
    if (settings.qSettings.runName == null)
      settings.qSettings.runName = FilenameUtils.removeExtension(scripts.head.getName)

    qGraph.settings = settings

    val allQScripts = pluginManager.createAllTypes();
    for (script <- allQScripts) {
      logger.info("Scripting " + pluginManager.getName(script.getClass.asSubclass(classOf[QScript])))
      loadArgumentsIntoObject(script)
      script.qSettings = settings.qSettings
      try {
        script.script()
      } catch {
        case e: Exception =>
          throw new UserException.CannotExecuteQScript(script.getClass.getSimpleName + ".script() threw the following exception: " + e, e)
      }
      script.functions.foreach(qGraph.add(_))
      logger.info("Added " + script.functions.size + " functions")
    }

    // Execute the job graph
    qGraph.run()

    val functionsAndStatus = qGraph.getFunctionsAndStatus
    val success = qGraph.success

    // walk over each script, calling onExecutionDone
    for (script <- allQScripts) {
      val scriptFunctions = functionsAndStatus.filterKeys(f => script.functions.contains(f))
      script.onExecutionDone(scriptFunctions, success)
    }

    logger.info("Script %s with %d total jobs".format(if (success) "completed successfully" else "failed", functionsAndStatus.size))

    if (!settings.disableJobReport) {
      val jobStringName = {
        if (settings.jobReportFile != null)
          settings.jobReportFile
        else
          settings.qSettings.runName + ".jobreport.txt"
      }

      if (!shuttingDown) {
        val reportFile = IOUtils.absolute(settings.qSettings.runDirectory, jobStringName)
        logger.info("Writing JobLogging GATKReport to file " + reportFile)
        QJobReport.printReport(functionsAndStatus, reportFile)

        if (settings.run) {
          val pdfFile = IOUtils.absolute(settings.qSettings.runDirectory, FilenameUtils.removeExtension(jobStringName) + ".pdf")
          logger.info("Plotting JobLogging GATKReport to file " + pdfFile)
          QJobReport.plotReport(reportFile, pdfFile)
        }
      }
    }

    if (!qGraph.success) {
      logger.info("Done with errors")
      qGraph.logFailed()
      1
    } else {
      0
    }
  }

  /**
   * Returns true as QScripts are located and compiled.
   * @return true
   */
  override def canAddArgumentsDynamically = true

  /**
   * Returns the list of QScripts passed in via -S so that their
   * arguments can be inspected before QScript.script is called.
   * @return Array of QScripts passed in.
   */
  override def getArgumentSources =
    pluginManager.getPlugins.toIterable.toArray.asInstanceOf[Array[Class[_]]]

  /**
   * Returns the name of a QScript
   * @return The name of a QScript
   */
  override def getArgumentSourceName(source: Class[_]) =
    pluginManager.getName(source.asSubclass(classOf[QScript]))

  /**
   * Returns a ScalaCompoundArgumentTypeDescriptor that can parse argument sources into scala collections.
   * @return a ScalaCompoundArgumentTypeDescriptor
   */
  override def getArgumentTypeDescriptors =
    Arrays.asList(new ScalaCompoundArgumentTypeDescriptor)

  override def getApplicationDetails : ApplicationDetails = {
    new ApplicationDetails(createQueueHeader(),
                           Seq.empty[String],
                           ApplicationDetails.createDefaultRunningInstructions(getClass.asInstanceOf[Class[CommandLineProgram]]),
                           "")
  }

  private def createQueueHeader() : Seq[String] = {
    Seq(String.format("Queue v%s, Compiled %s", getQueueVersion, getBuildTimestamp),
         "Copyright (c) 2012 The Broad Institute",
         "Please view our documentation at http://www.broadinstitute.org/gsa/wiki",
         "For support, please view our support site at http://getsatisfaction.com/gsa")
  }

  private def getQueueVersion : String = {
    val stingResources : ResourceBundle = TextFormattingUtils.loadResourceBundle("StingText")

    if ( stingResources.containsKey("org.broadinstitute.sting.queue.QueueVersion.version") ) {
      stingResources.getString("org.broadinstitute.sting.queue.QueueVersion.version")
    }
    else {
      "<unknown>"
    }
  }

  private def getBuildTimestamp : String = {
    val stingResources : ResourceBundle = TextFormattingUtils.loadResourceBundle("StingText")

    if ( stingResources.containsKey("build.timestamp") ) {
      stingResources.getString("build.timestamp")
    }
    else {
      "<unknown>"
    }
  }

  def shutdown() = {
    shuttingDown = true
    qGraph.shutdown()
    if (qScriptClasses != null) IOUtils.tryDelete(qScriptClasses)
  }
}
