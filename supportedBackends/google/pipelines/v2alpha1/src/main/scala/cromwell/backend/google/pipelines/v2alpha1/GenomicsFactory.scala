package cromwell.backend.google.pipelines.v2alpha1

import java.net.URL

import com.google.api.client.http.HttpRequestInitializer
import com.google.api.services.genomics.v2alpha1.Genomics
import com.google.api.services.genomics.v2alpha1.model._
import cromwell.backend.google.pipelines.common.api.PipelinesApiRequestFactory.CreatePipelineParameters
import cromwell.backend.google.pipelines.common.api.{PipelinesApiFactoryInterface, PipelinesApiRequestFactory}
import cromwell.backend.google.pipelines.common.{PipelinesApiFileInput, PipelinesApiFileOutput, PipelinesApiLiteralInput}
import cromwell.backend.google.pipelines.v2alpha1.PipelinesConversions._
import cromwell.backend.google.pipelines.v2alpha1.api.{ActionBuilder, DeLocalization, Localization}
import cromwell.backend.standard.StandardAsyncJob
import cromwell.cloudsupport.gcp.auth.GoogleAuthMode

import scala.collection.JavaConverters._

case class GenomicsFactory(applicationName: String, authMode: GoogleAuthMode, endpointUrl: URL) extends PipelinesApiFactoryInterface
  with Localization
  with DeLocalization {

  override def build(initializer: HttpRequestInitializer): PipelinesApiRequestFactory = new PipelinesApiRequestFactory {
    val genomics = new Genomics.Builder(
      GoogleAuthMode.httpTransport,
      GoogleAuthMode.jsonFactory,
      initializer)
      .setApplicationName(applicationName)
      .setRootUrl(endpointUrl.toString)
      .build

    override def cancelRequest(job: StandardAsyncJob) = {
      genomics.projects().operations().cancel(job.jobId, new CancelOperationRequest()).buildHttpRequest()
    }

    override def getRequest(job: StandardAsyncJob) = {
      genomics.projects().operations().get(job.jobId).buildHttpRequest()
    }
    
    override def runRequest(createPipelineParameters: CreatePipelineParameters) = {
      val allInputOutputParameters = createPipelineParameters.allParameters

      // Disks defined in the runtime attributes
      val disks = createPipelineParameters.toDisks
      // Mounts for disks defined in the runtime attributes
      val mounts = createPipelineParameters.toMounts
      
      val localization: List[Action] = localizeActions(createPipelineParameters, mounts)
      // localization.size + 1 because action indices are 1-based and the next action after localization will be the user's
      val deLocalization: List[Action] = deLocalizeActions(createPipelineParameters, mounts, localization.size + 1)

      val environment = allInputOutputParameters.flatMap({
        case fileInput: PipelinesApiFileInput => fileInput.toEnvironment
        case fileOutput: PipelinesApiFileOutput => fileOutput.toEnvironment
        case literal: PipelinesApiLiteralInput => literal.toEnvironment
      }).toMap.asJava

      val userAction = ActionBuilder.userAction(createPipelineParameters.dockerImage, createPipelineParameters.commandLine, mounts, createPipelineParameters.labels.asMap)

      val serviceAccount = new ServiceAccount()
        .setEmail(createPipelineParameters.computeServiceAccount)
        .setScopes(PipelinesApiFactoryInterface.GenomicsScopes)

      val virtualMachine = new VirtualMachine()
        .setDisks(disks.asJava)
        .setPreemptible(createPipelineParameters.preemptible)
        .setServiceAccount(serviceAccount)
        .setMachineType(createPipelineParameters.runtimeAttributes.toMachineType)
        .setBootDiskSizeGb(createPipelineParameters.runtimeAttributes.bootDiskSize)
        .setLabels(createPipelineParameters.labels.asJavaMap)

      val resources = new Resources()
        .setProjectId(createPipelineParameters.projectId)
        .setZones(createPipelineParameters.runtimeAttributes.zones.asJava)
        .setVirtualMachine(virtualMachine)

      val pipeline = new Pipeline()
        .setResources(resources)
        .setActions((localization ++ List(userAction) ++ deLocalization).asJava)
        .setEnvironment(environment)

      val pipelineRequest = new RunPipelineRequest()
        .setPipeline(pipeline)
        .setLabels(createPipelineParameters.labels.asJavaMap)

      genomics.pipelines().run(pipelineRequest).buildHttpRequest()
    }
  }
}