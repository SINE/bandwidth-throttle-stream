import { resolve } from "https://deno.land/std@0.200.0/path/resolve.ts";
import IBaseTransformStreamConstructorParams from '../Interfaces/IBaseTransformStreamConstructorParams.ts';

class BaseTransformStream extends TransformStream<Uint8Array, Uint8Array> {
    private controller: TransformStreamDefaultController<
        Uint8Array
    > | null = null;

    constructor({transform, flush}: IBaseTransformStreamConstructorParams) {
        super({
            transform: (chunk, controller) => {
                    this.controller = controller;

                    return transform(chunk);
            },
            flush
        });
    }

}

export default BaseTransformStream;
