package blobstore.gcs

import shapeless.Witness

package object experiment {

  type GcsProtocol = Witness.`"gs"`.T

}
